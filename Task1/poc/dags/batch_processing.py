from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("BATCH_DATA_DIR", "/opt/airflow/data"))
STAGING_DIR = DATA_DIR / "staging"
REPORT_DIR = DATA_DIR / "report"
CSV_FILE = DATA_DIR / "delivery_statuses.csv"
SUMMARY_FILE = DATA_DIR / "analysis_summary.json"
FLAKY_COUNTER_FILE = DATA_DIR / "flaky_attempts.txt"

ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "admin@example.com")
SOURCE_PG_CONN_ID = os.environ.get("SOURCE_PG_CONN_ID", "source_postgres")
DEFAULT_ANOMALY_THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD", "0.10"))
DEFAULT_FLAKY_FAILURES = int(os.environ.get("FLAKY_MAX_FAILURES", "2"))
SOURCE_TABLES = ("customers", "orders", "payments")

default_args = {
    "owner": "marketing-data",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=30),
}


def flaky_step_impl() -> str:
    current = 0
    if FLAKY_COUNTER_FILE.exists():
        current = int(FLAKY_COUNTER_FILE.read_text(encoding="utf-8").strip() or "0")

    if current < DEFAULT_FLAKY_FAILURES:
        FLAKY_COUNTER_FILE.write_text(str(current + 1), encoding="utf-8")
        raise RuntimeError(
            f"Имитация временного сбоя #{current + 1}/{DEFAULT_FLAKY_FAILURES}: "
            "задача будет автоматически повторена по retry-политике"
        )

    log.info("Временный сбой устранён после %s повтор(ов)", current)
    return "flaky_ok"


def choose_branch_impl(**context) -> str:
    summary = context["ti"].xcom_pull(task_ids="spark_analyze") or {}
    failed_rate = float(summary.get("failed_rate", 0.0))
    threshold = float(summary.get("threshold", DEFAULT_ANOMALY_THRESHOLD))

    if failed_rate > threshold:
        log.warning(
            "Ветвление -> alert_anomaly (failed_rate=%.4f > threshold=%.4f)", failed_rate, threshold
        )
        return "alert_anomaly"

    log.info(
        "Ветвление -> normal_report (failed_rate=%.4f <= threshold=%.4f)", failed_rate, threshold
    )
    return "normal_report"


@task
def reset_flaky_counter() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FLAKY_COUNTER_FILE.write_text("0", encoding="utf-8")
    return "reset"


@task
def read_delivery_csv() -> dict:
    import pandas as pd

    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV не найден: {CSV_FILE}. Запустите сервис data-init.")

    df = pd.read_csv(CSV_FILE)
    required = {"order_id", "delivery_status", "carrier", "updated_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В CSV отсутствуют колонки: {sorted(missing)}")

    stats = {
        "path": str(CSV_FILE),
        "rows": int(len(df)),
        "statuses": {str(k): int(v) for k, v in df["delivery_status"].value_counts().items()},
    }
    log.info("CSV прочитан: %s", stats)
    return stats


@task
def extract_postgres() -> dict:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=SOURCE_PG_CONN_ID)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for table in SOURCE_TABLES:
        df = hook.get_pandas_df(f"SELECT * FROM {table}")
        df.to_csv(STAGING_DIR / f"{table}.csv", index=False)
        counts[table] = int(len(df))

    log.info("PostgreSQL выгружен в staging: %s", counts)
    return counts


@task
def spark_analyze() -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    context = get_current_context()
    threshold = float(context["params"].get("anomaly_threshold", DEFAULT_ANOMALY_THRESHOLD))

    if not (STAGING_DIR / "orders.csv").exists():
        raise FileNotFoundError("Нет staging-данных: сначала выполните extract_postgres.")

    spark = (
        SparkSession.builder.appName("marketing_batch_poc")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        customers = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(STAGING_DIR / "customers.csv"))
            .select("user_id", "region")
        )
        orders = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(STAGING_DIR / "orders.csv"))
            .select("order_id", "user_id", "amount", "status")
            .withColumnRenamed("amount", "order_amount")
            .withColumnRenamed("status", "order_status")
        )
        payments = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(STAGING_DIR / "payments.csv"))
            .select("order_id", "status")
            .withColumnRenamed("status", "payment_status")
        )
        deliveries = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(CSV_FILE))
            .select("order_id", "delivery_status", "carrier")
        )

        joined = (
            orders.join(customers, "user_id", "left")
            .join(payments, "order_id", "left")
            .join(deliveries, "order_id", "left")
        )

        failed_expr = F.sum(F.when(F.col("delivery_status") == "failed", 1).otherwise(0))
        delivered_expr = F.sum(F.when(F.col("delivery_status") == "delivered", 1).otherwise(0))

        by_region = (
            joined.groupBy("region")
            .agg(
                F.count("order_id").alias("orders"),
                failed_expr.alias("failed_deliveries"),
                delivered_expr.alias("delivered"),
                F.round(F.sum("order_amount"), 2).alias("revenue"),
            )
            .orderBy(F.col("failed_deliveries").desc())
        )

        total_orders = joined.count()
        failed_deliveries = joined.filter(F.col("delivery_status") == "failed").count()
        failed_rate = round(failed_deliveries / total_orders, 4) if total_orders else 0.0

        out_dir = REPORT_DIR / "delivery_by_region"
        by_region.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))

        summary = {
            "total_orders": int(total_orders),
            "failed_deliveries": int(failed_deliveries),
            "failed_rate": float(failed_rate),
            "threshold": float(threshold),
            "branch": "alert_anomaly" if failed_rate > threshold else "normal_report",
            "report_path": str(out_dir),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SUMMARY_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        log.info("Spark-анализ завершён: %s", summary)
        return summary
    finally:
        spark.stop()


@task
def alert_anomaly() -> str:
    message = "Аномалия: доля неуспешных доставок превысила порог. Требуется внимание маркетинга."
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "anomaly_alert.txt").write_text(message, encoding="utf-8")
    log.warning(message)
    return "alert_anomaly"


@task
def normal_report() -> str:
    message = "Доля неуспешных доставок в пределах нормы. Отчёт сформирован."
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "normal_report.txt").write_text(message, encoding="utf-8")
    log.info(message)
    return "normal_report"


with DAG(
    dag_id="batch_processing_poc",
    description="POC: CSV + PostgreSQL -> PySpark -> ветвление -> email/retry",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "anomaly_threshold": Param(
            DEFAULT_ANOMALY_THRESHOLD,
            type="number",
            title="Порог аномалии",
            description=(
                "Если доля неуспешных доставок выше порога - пайплайн идёт в ветку alert_anomaly. "
                "Для демонстрации второй ветки запустите DAG с маленьким порогом, например 0.001."
            ),
        ),
    },
    tags=["poc", "batch", "spark"],
) as dag:
    reset = reset_flaky_counter()
    csv_info = read_delivery_csv()
    pg_info = extract_postgres()
    analysis = spark_analyze()

    branch = BranchPythonOperator(task_id="choose_branch", python_callable=choose_branch_impl)
    alert = alert_anomaly()
    normal = normal_report()

    flaky = PythonOperator(
        task_id="flaky_step",
        python_callable=flaky_step_impl,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=[ALERT_EMAIL],
        subject="[Airflow] Пайплайн завершён успешно",
        html_content=(
            "<h3>Пайплайн пакетной обработки завершён успешно</h3>"
            "<p><b>DAG:</b> {{ dag.dag_id }}<br>"
            "<b>Run:</b> {{ run_id }}<br>"
            "<b>Logical date:</b> {{ ds }}</p>"
            "<p><b>Результат анализа (spark_analyze):</b></p>"
            "<pre>{{ ti.xcom_pull(task_ids='spark_analyze') }}</pre>"
            "<p>Отчёт: <code>data/report/delivery_by_region</code></p>"
        ),
    )

    reset >> [csv_info, pg_info]
    [csv_info, pg_info] >> analysis
    analysis >> branch
    branch >> [alert, normal]
    [alert, normal] >> flaky
    flaky >> notify_success
