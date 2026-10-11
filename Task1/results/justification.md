# Обоснование выбора решения для пакетной обработки данных

## Контекст и требования

Маркетинговому отделу нужно регулярно объединять данные клиентов из разнородных источников и
формировать отчёты:

- Файловое хранилище CSV - статусы доставок пользователей;
- PostgreSQL - таблицы заказов, платежей и расширенные данные пользователей;
- Kafka - цепочка событий по модификации заказов.

Ожидаемый объём данных - ~1 000 000 записей за один запуск пайплайна. Текущая система не справляется с
нагрузкой и функциональностью, поэтому требуется выбрать и обосновать технологическое решение.
В таблице ниже приведен перечень требований:

| № | Требование | Что это означает для инструмента |
|---|---|---|
| 1 | Гибко формировать пайплайн | Декларативное описание зависимостей, переиспользуемые шаги |
| 2 | Интеграция с внешними API, BigQuery, Redshift, Kafka, Spark | Готовые коннекторы, а не собственный код с нуля |
| 3 | Ветвление, условные операторы, event-triggers | Возможность задать операторы ветвления, событийные запуски |
| 4 | Fallback-logic, retry, email-уведомления "из коробки" | Встроенные политики повторов и колбэки уведомлений |
| 5 | Встроенный мониторинг и оповещения | Web UI, метрики, алерты по статусам задач |

## Apache Airflow как оркестратор пакетной обработки

В качестве технологическо решения предлагается использование Apache Airflow как оркестратора пакетной обработки данных.
Важно: Airflow - это оркестратор, а не вычислительный движок. Он отвечает за
расписание, зависимости, ветвление, retry, мониторинг, уведомления и интеграции, а тяжёлые
вычисления передаёт специализированным системам: Spark, BigQuery /
Redshift. Такое решение позволяет гибко формировать пайплайн и интегрировать его с внешними системами. Для 1 млн записей за запуск
такая схема даёт правильное разделение ответственности: Airflow управляет потоком, а объём данных
"переваривают" движки, которые для этого созданы.

Далее разберем соответствие решения требованиям по пунктам.

### Гибкое формирование пайплайна

- DAG как код на Python: линейные и разветвлённые графы зависимостей, динамическая генерация
  задач (dynamic task mapping), TaskFlow API (@task) для читаемого кода.
- Параметризация: params, Jinja-шаблоны, XCom для передачи данных между шагами.
- Переиспользование: общие библиотеки задач, DAG-фабрики; однотипные пайплайны описываются
  декларативно (YAML/шаблоны) без копирования кода.

### Интеграции: готовые модули для BigQuery, Redshift, Kafka, Spark и внешних API

Airflow построен на провайдерах - отдельных pip-пакетах с готовыми операторами, хуками и
триггерами. Разработку ускоряют именно они:

| Система | Пакет-провайдер (pip) | Готовые операторы/хуки |
|---|---|---|
| BigQuery (GCP) | `apache-airflow-providers-google` | `BigQueryInsertJobOperator`, `GCSToBigQueryOperator`, `BigQueryToGCSOperator`, `BigQueryCreateEmptyTableOperator`, `BigQueryHook` |
| Redshift (AWS) | `apache-airflow-providers-amazon` | `RedshiftSQLOperator`, `S3ToRedshiftOperator`, `RedshiftToS3Operator`, `RedshiftCreateClusterOperator`, `RedshiftPauseClusterOperator`, `RedshiftHook` |
| Kafka | `apache-airflow-providers-apache-kafka` | `KafkaProducerOperator`, `KafkaConsumerOperator`, `KafkaAdminClientHook`, deferrable `AwaitMessageTrigger` |
| Spark | `apache-airflow-providers-apache-spark` (или `-databricks`, `-apache-livy`) | `SparkSubmitOperator`, `SparkJDBCOperator`, `SparkSqlOperator`, `KubernetesPodOperator` для Spark-on-k8s |
| Внешние API | `apache-airflow-providers-http` | `HttpOperator`, `HttpSensor`, `HttpHook` (REST) |
| PostgreSQL | `apache-airflow-providers-postgres` | `PostgresOperator`, `PostgresHook`, `PostgresToGCSOperator` |
| Email (SMTP) | `apache-airflow-providers-smtp` | `EmailOperator`, `SmtpHook` (в 2.7+ вынесены из ядра) |

Таким образом, для всех основных систем из задания существуют официальные или поддерживаемые сообществом модули. 
Они существенно сокращают объём собственного интеграционного кода: обычно достаточно настроить подключение и использовать готовый 
оператор/hook, а специфическую бизнес-логику при необходимости реализовать отдельно.

### Ветвление, условные операторы и event-triggers - поддерживаются

- Ветвление и условия: `BranchPythonOperator`, `BranchSQLOperator`, `BranchDateTimeOperator`,
  `ShortCircuitOperator`, TaskFlow-аналог `@task.branch` (в Airflow 3 - `PythonBranchOperator`).
- Trigger rules на уровне задач: `all_success`, `all_done`, `one_success`, `one_failed`,
  `all_failed`, `none_failed_min_one_success` - позволяют строить альтернативные ветки.
- Event-driven запуск:
  - Datasets / Assets (Airflow 2.4+ / 3.0) - запуск DAG как реакция на обновление данных;
  - TriggerDagRunOperator - запуск другого DAG по событию;
  - Sensors - `FileSensor`, `S3KeySensor`, `SqlSensor`, `ExternalTaskSensor`; Kafka-провайдер
    даёт deferrable-триггер на ожидание сообщения в топике;
  - deferrable (асинхронные) операторы - ожидание внешних событий без удержания воркера.

### Retry, fallback и email-уведомления

Всё это - встроенные возможности ядра, задаются в `default_args` DAG и на уровне отдельных задач:

- Retry-политика: `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay` -
  автоматические повторы с настраиваемой задержкой.
- Fallback-логика:
  - альтернативные пути через trigger rules и ветвление (например, при сбое Spark-ветки -
    переход на резервную обработку);
  - `on_failure_callback`, `on_retry_callback`, `on_success_callback` - реакции на события жизни DAG;
  - SLA-miss и `sla_miss_callback` - реакция на нарушение сроков.
- Email-уведомления: Airflow поддерживает отправку email через SMTP и предоставляет `EmailOperator` и callback-механизмы. Однако SMTP-сервер и его параметры необходимо настроить - почтовая инфраструктура не появляется автоматически после установки Airflow.

### Мониторинг и оповещения

Возможности Airflow из коробки:
- Web UI: Graph/Grid/Gantt, история запусков, длительности, логи задач и состояние DAG/tasks;
- health endpoints и базовые метрики состояния;
- callbacks и notification-механизмы для реакции на состояния задач и DAG.

Production-расширения:
- метрики через StatsD/Prometheus с визуализацией в Grafana;
- централизованное хранение логов в S3/GCS/ELK;
- OpenTelemetry для трассировки;
- внешние системы оповещений: Slack/PagerDuty и т. п.

Таким образом, Airflow предоставляет встроенный UI и механизмы контроля выполнения, а полноценный observability stack для production обычно строится с использованием внешних систем мониторинга.

### Масштаб ~1 млн записей за запуск

1 млн записей - умеренный объём. Airflow не обрабатывает данные сам, поэтому масштабирование
решается так:

- Тяжёлые преобразования - на Spark (`SparkSubmitOperator` / `KubernetesPodOperator`) или внутри
  BigQuery/Redshift (ELT на стороне хранилища);
- Airflow оркестрирует шаги, объединение и агрегацию выполняет движок;
- Масштабирование исполнения - смена executor (LocalExecutor → CeleryExecutor →
  KubernetesExecutor), autoscale воркеров, настройка `parallelism`/`max_active_tasks`.

## Облачное развёртывание (Kubernetes + официальный Apache Airflow Helm Chart)

Предлагается вариант развёртывания Airflow в Kubernetes - он нейтрален к облаку и переносится между
GCP/AWS/on-prem.

Практика развёртывания:

- Helm: официальный community Helm Chart `apache-airflow/airflow` используется для развёртывания Airflow в Kubernetes; задаются executor, ресурсы, секреты и параметры среды.
- Сборка и доставка: CI/CD собирает образ с зависимостями (провайдеры, при необходимости `pyspark`, Java и системные библиотеки) и публикует его в container registry, после чего Helm-деплой обновляет среду.

  webserver/scheduler/workers, выбирает executor, задаёт ресурсы и секреты.
- Изоляция задач: `KubernetesExecutor` / `KubernetesPodOperator` - каждая задача в отдельном pod,
  тяжёлый Spark-шаг не мешает лёгким задачам.
- GitOps: DAG-и подтягиваются sidecar'ом `git-sync` из репозитория - версионирование и ревью
  пайплайнов.
- Доступ к данным: `Workload Identity` (GCP) / `IRSA` (AWS) вместо хранения ключей; секреты - через
  k8s/External Secrets.
- Managed-альтернативы: AWS MWAA и GCP Cloud Composer позволяют снизить операционные затраты,
  сохраняя модель DAG Airflow, но уменьшают свободу выбора инфраструктуры и конфигурации. Поэтому
  основной вариант для демонстрации и архитектурного обоснования - self-managed Airflow в Kubernetes.
  Коммерческий managed Airflow (например, Astronomer) при необходимости можно рассматривать как
  отдельную альтернативу, но он не является обязательной частью предлагаемого решения.

## Ограничения и допущения

- Airflow не заменяет вычислительный движок: для больших объёмов нужен Spark или что-то подобное.
- Airflow не предназначен для потоковой (streaming) обработки. В данном решении Kafka используется как источник событий и потенциальный триггер batch-пайплайна: события за заданный временной интервал или событие поступления данных инициируют/обеспечивают последующую пакетную обработку. Непрерывная stream processing логика остаётся задачей Kafka Streams/Spark Structured Streaming или другого специализированного движка.
- Версии Airflow и провайдеров нужно фиксировать, чтобы обновления не ломали пайплайны.

## Сравнение с альтернативами

Перед окончательным выбором полезно сравнить Airflow с другими распространёнными оркестраторами:

| Решение | Сильные стороны | Ограничения в данном кейсе |
|---|---|---|
| Apache Airflow | Зрелая DAG-модель, большое количество provider packages, интеграции с Kafka/Spark/BigQuery/Redshift, retry, branching, sensors, monitoring | Требует управления инфраструктурой при self-managed deployment |
| Prefect | Python-first API, удобная разработка и современный подход к оркестрации | Для данного набора систем и требований Airflow имеет более привычную и зрелую экосистему интеграций |
| Dagster | Сильная data-centric модель, типизация и управление data assets | Может быть избыточен для задачи, где основной акцент - оркестрация разнородных внешних систем |
| Luigi | Простая модель зависимостей и DAG | Менее богатая экосистема интеграций и operational tooling |
| Cron + Python | Минимальная инфраструктура и низкий порог входа | Нет полноценного DAG UI, удобного dependency management, retry/branching/monitoring на уровне платформы |

Почему выбран Airflow: именно сочетание DAG orchestration, готовых provider packages для требуемых систем,
branching/event-driven механизмов, retry, уведомлений и развитого Web UI наиболее полно соответствует
требованиям. При этом Airflow не пытается заменить Spark или аналитические хранилища, а оркестрирует их использование.

## Резюме

Apache Airflow закрывает все требования, предъявляемые к решению:

1. Гибкое описание пайплайна (DAG как код, TaskFlow, параметризация);
2. Готовые провайдеры под BigQuery, Redshift, Kafka, Spark, HTTP и PostgreSQL;
3. Ветвление, условные операторы и event-triggers (Branch/ShortCircuit/sensors/datasets);
4. Встроенный retry; fallback реализуется средствами DAG (trigger rules/branching/callbacks), а email - через SMTP и соответствующий provider;
5. Встроенный мониторинг и оповещения.

Заявленный объём (~1 млн записей за запуск) не является для Airflow проблемой, поскольку тяжёлые
вычисления выносятся в Spark или подобную систему, а Airflow отвечает за оркестрацию. 
Airflow выбран в качестве технологического решения.