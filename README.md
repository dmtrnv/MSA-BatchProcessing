# MSA-BatchProcessing — пакетная обработка данных в микросервисной архитектуре

Проект последовательно разбирает задачу организации пакетной (batch) обработки данных
в микросервисной среде: от анализа технологий и POC до проектных решений, оформленных как ADR.
Рассматриваются сценарии агрегации данных из разнородных источников (CSV, PostgreSQL, Kafka),
ночной генерации прайс-листов и обработки складских отчётов, а также контур наблюдаемости.
Диаграммы подготовлены в нотации C4.

## Результаты по этапам

### 1. POC оркестрации пакетной обработки на Apache Airflow

Рабочий прототип пайплайна: объединение данных из CSV и PostgreSQL, обработка PySpark,
ветвление по порогу аномалии, retry-политика и email-уведомления.

| Артефакт | Тема | Ссылка |
|----------|------|--------|
| Обоснование | Выбор Apache Airflow и соответствие требованиям | [justification.md](Task1/results/justification.md) |
| DAG | Пайплайн (CSV + PostgreSQL → Spark → ветвление → email/retry) | [batch_processing.py](Task1/poc/dags/batch_processing.py) |
| Скрипт данных | Генератор исходных данных | [generate_source_data.py](Task1/poc/scripts/generate_source_data.py) |
| Окружение | Docker Compose для локального запуска Airflow | [docker-compose.yml](Task1/poc/docker/docker-compose.yml) |

Демонстрация работы: [скриншоты](Task1/results/screenshots) и [видео](Task1/results/video/airflow-process.mp4).

### 2. Дизайн модуля пакетной обработки данных

Проектирование модуля ночной генерации B2B-прайс-листов: сравнительный анализ
Spring Batch, Airflow, K8s Job и Spark, выбор решения и верхнеуровневый план реализации.

| Артефакт | Тема | Ссылка |
|----------|------|--------|
| Обоснование | Сравнение технологий и выбор Kubernetes CronJob | [justification.md](Task2/results/justification.md) |
| План | Верхнеуровневый план конфигурации и имплементации | [implementation-plan.md](Task2/results/implementation-plan.md) |

| Диаграмма | PlantUML | PNG |
|-----------|----------|-----|
| C4-диаграмма контекста (C1) To Be | [С1-to-be.puml](Task2/results/С1-to-be.puml) | [С1-to-be.png](Task2/results/С1-to-be.png) |

### 3. POC Kubernetes CronJob для пакетного экспорта

Прототип ночного экспорта данных: Go-экспортёр выгружает таблицу из PostgreSQL в CSV
(с gzip), публикует результат в Object Storage (MinIO/S3), запускается по Kubernetes CronJob
с retry, TTL и ограничениями по ресурсам.

| Артефакт | Тема | Ссылка |
|----------|------|--------|
| Экспортёр | Go-приложение выгрузки (COPY TO CSV → S3) | [export.go](Task3/poc/exporter/internal/export/export.go) |
| Конфигурация | Параметры подключения и выгрузки из env | [config.go](Task3/poc/exporter/internal/config/config.go) |
| CronJob | Манифест расписания запуска | [cronjob.yaml](Task3/results/cronjob.yaml) |
| Образ | Multi-stage Dockerfile (distroless, non-root) | [Dockerfile](Task3/results/Dockerfile) |
| Скрипты | Сборка, деплой и ручной запуск Job | [scripts](Task3/poc/scripts) |
| Манифесты | Kubernetes-ресурсы (PostgreSQL, MinIO, exporter) | [k8s](Task3/poc/k8s) |

Демонстрация работы: [скриншоты](Task3/results/screenshots).

### 4. ADR: пакетная обработка загрузок остатков (Spring Batch)

Архитектурное решение по переводу загрузки складских отчётов в пакетный режим
с использованием Spring Batch: chunk processing, checkpoint/restart, retry/skip,
горизонтальное масштабирование до 100–150 параллельных загрузок (очередь Kafka + пул worker'ов + partitioning).

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-001 | Пакетная обработка загрузок остатков с использованием Spring Batch | [ADR.md](Task4/results/ADR.md) |

| Диаграмма | PlantUML | PNG |
|-----------|----------|-----|
| Диаграмма контейнеров (C2) | [С2.puml](Task4/results/С2.puml) | [С2.png](Task4/results/С2.png) |

### 5. ADR: единый контур наблюдаемости

Архитектурное решение по построению наблюдаемости (метрики, логи, алерты) на базе
Prometheus + Grafana + Loki + Alertmanager, с каталогом метрик, SLI/SLO и правилами алертов.

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-002 | Единый контур наблюдаемости (метрики, логи, алерты) | [ADR.md](Task5/results/ADR.md) |
| Каталог | Обоснование выбора метрик, логов и алертов | [metrics.md](Task5/results/metrics.md) |

| Диаграмма | PlantUML | PNG |
|-----------|----------|-----|
| Диаграмма контейнеров (C2) с контуром наблюдаемости | [C2.puml](Task5/results/C2.puml) | [C2.png](Task5/results/C2.png) |

## Как читать артефакты

Каждая задача содержит каталог `results/` с итоговыми документами, а для демонстрационных
задач (Task1, Task3) дополнительно — каталог `poc/` с рабочей реализацией (код, манифесты,
скрипты) и инфраструктурой локального запуска. ADR в Task4 и Task5 содержат контекст,
требования, рассмотренные альтернативы, принятое решение и его следствия. Диаграммы
представлены исходником PlantUML (`.puml`) и готовым рендером (`.png`).
