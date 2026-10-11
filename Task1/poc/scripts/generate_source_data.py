from __future__ import annotations
import csv
import os
import random
import sys
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

ROW_COUNT = int(os.environ.get("ROW_COUNT", "10000"))
CUSTOMERS = int(os.environ.get("CUSTOMERS", str(min(2000, ROW_COUNT))))
SEED = int(os.environ.get("SEED", "42"))

DATA_DIR = os.environ.get("BATCH_DATA_DIR", "/opt/airflow/data")
CSV_PATH = os.path.join(DATA_DIR, "delivery_statuses.csv")

DB = {
    "host": os.environ.get("DB_HOST", "postgres-source"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "batch"),
    "user": os.environ.get("DB_USER", "batch"),
    "password": os.environ.get("DB_PASSWORD", "batch"),
}

REGIONS = ["EU-WEST", "EU-EAST", "US-EAST", "US-WEST", "APAC", "LATAM"]
ORDER_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]
ORDER_WEIGHTS = [5, 15, 25, 45, 10]
DELIVERY_STATUSES = ["delivered", "in_transit", "failed", "pending"]
DELIVERY_WEIGHTS = [60, 20, 12, 8]
CARRIERS = ["DHL", "UPS", "FedEx", "DPD", "PostNL"]

DDL = """
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    user_id     INTEGER PRIMARY KEY,
    email       TEXT NOT NULL,
    name        TEXT NOT NULL,
    region      TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL
);

CREATE TABLE orders (
    order_id    INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES customers(user_id),
    amount      NUMERIC(12,2) NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL
);

CREATE TABLE payments (
    payment_id  INTEGER PRIMARY KEY,
    order_id    INTEGER NOT NULL REFERENCES orders(order_id),
    amount      NUMERIC(12,2) NOT NULL,
    status      TEXT NOT NULL,
    paid_at     TIMESTAMP
);
"""


def main() -> int:
    random.seed(SEED)
    os.makedirs(DATA_DIR, exist_ok=True)
    base = datetime(2024, 1, 1, 0, 0, 0)

    customers = []
    for user_id in range(1, CUSTOMERS + 1):
        created = base + timedelta(days=random.randint(0, 30), seconds=random.randint(0, 86399))
        customers.append(
            (user_id, f"user{user_id}@example.com", f"Customer {user_id}", random.choice(REGIONS), created)
        )

    orders = []
    payments = []
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["order_id", "delivery_status", "carrier", "updated_at"])
        for order_id in range(1, ROW_COUNT + 1):
            user_id = random.randint(1, CUSTOMERS)
            amount = round(random.uniform(10.0, 2000.0), 2)
            order_status = random.choices(ORDER_STATUSES, weights=ORDER_WEIGHTS, k=1)[0]
            created = base + timedelta(days=random.randint(0, 30), seconds=random.randint(0, 86399))
            orders.append((order_id, user_id, amount, order_status, created))

            if order_status in ("paid", "shipped", "delivered"):
                payment_status = "paid"
            else:
                payment_status = random.choice(["pending", "failed"])
            paid_at = (
                created + timedelta(hours=random.randint(1, 72)) if payment_status == "paid" else None
            )
            payments.append((order_id, order_id, amount, payment_status, paid_at))

            delivery_status = random.choices(DELIVERY_STATUSES, weights=DELIVERY_WEIGHTS, k=1)[0]
            updated_at = created + timedelta(hours=random.randint(1, 96))
            writer.writerow([order_id, delivery_status, random.choice(CARRIERS), updated_at.isoformat(sep=" ")])

    conn = psycopg2.connect(**DB)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
            execute_values(
                cur,
                "INSERT INTO customers (user_id, email, name, region, created_at) VALUES %s",
                customers,
            )
            execute_values(
                cur,
                "INSERT INTO orders (order_id, user_id, amount, status, created_at) VALUES %s",
                orders,
            )
            execute_values(
                cur,
                "INSERT INTO payments (payment_id, order_id, amount, status, paid_at) VALUES %s",
                payments,
            )
    finally:
        conn.close()

    print(
        "Готово: "
        f"customers={len(customers)}, orders={len(orders)}, payments={len(payments)}, "
        f"csv_rows={ROW_COUNT} -> {CSV_PATH}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
