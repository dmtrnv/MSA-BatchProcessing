# Обоснование выбора метрик, логов и алертов для наблюдаемости

Документ дополняет [ADR-002: Единый контур наблюдаемости (метрики, логи, алерты) на базе Prometheus/Grafana/Loki/Alertmanager](./ADR.md). 

Содержит каталог метрик по слоям, связь с SLI/SLO и правила алертов.

## Принципы выбора

1. **Связь с бизнес-целью.** Оставляем только метрики, объясняющие ключевые риски кейса:
   "отчёты висят", задержки обновления, сбои в пике.
2. **Три уровня детализации:** бизнес/SLI → приложение/сервис → инфраструктура/ресурсы.
3. **Гистограммы для latency**, чтобы считать p95/p99 и SLI, а не только средние.
4. **Кардинальность под контролем:** labels - только низкокардинальные (`status`, `jobName`, `step`,
   `warehouseId` ограниченно); `jobId`/`fileId` идут **в логи**, не в labels метрик.

## Каталог ключевых метрик

### Бизнес-уровень / SLI (интегральные)

| Метрика | Тип | Ед. | Источник | Назначение | SLI/SLO |
|---|---|---|---|---|---|
| `tw_report_processing_seconds` | histogram | с | Worker | Сквозное время обработки отчёта; p95 для отчёта 2000 строк | SLI latency. SLO: p95 ≤ 30 с для 2000 строк |
| `tw_import_job_total{status}` | counter | шт | Worker | Число заданий по статусу (SUCCEEDED/FAILED/PARTIAL) | SLI success rate. SLO ≥ 99.5% успешных |
| `tw_api_availability_ratio` | gauge/расчёт | ratio | API (5xx/успехи) | Доступность API приёма | SLI availability. SLO ≥ 99.9% |
| `tw_concurrent_imports` | gauge | шт | API/Worker | Число одновременных загрузок | Ёмкость против 100–150 в пик |

### Приём / Backend API

| Метрика | Тип | Ед. | Назначение |
|---|---|---|---|
| `http_server_requests_seconds{uri,status,method}` | histogram | с | Латентность API приёма (p95/p99), в т.ч. upload |
| `tw_upload_rejected_total{reason}` | counter | шт | Отклонённые файлы (формат/размер/права) - контроль качества на входе |
| `tw_job_publish_total{result}` | counter | шт | Успешные/неуспешные публикации в очередь (outbox) |
| `tw_api_errors_total{code}` | counter | шт | Ошибки API по кодам |

### Очередь Kafka (backpressure) - раннее предупреждение о перегрузке

| Метрика | Тип | Ед. | Назначение | Алерт |
|---|---|---|---|---|
| `kafka_consumergroup_lag{group,topic}` | gauge | шт | Consumer lag - главный SLI backpressure: сколько заданий не обработано | Warning при устойчивом росте, critical при > N |
| `kafka_consumergroup_lag_seconds{group,topic}` | gauge | с | Оценка времени отставания (lag / скорость потребления) - реальное ожидание | Critical при > T |
| `kafka_topic_partition_current_offset{topic,partition}` | gauge | шт | Текущий offset - скорость потребления/позиция группы | - |
| `kafka_topic_partition_under_replicated{topic}` | gauge | шт | Under-replicated партиции - здоровье брокеров/репликации | Critical при > 0 |
| `kafka_producer_record_error_total` | counter | шт | Ошибки публикации в топик (Kafka producer через outbox) | Warning при росте |

### Исполнение Spring Batch / Job

| Метрика | Тип | Ед. | Назначение |
|---|---|---|---|
| `spring_batch_job_seconds{jobName,status}` | histogram | с | Длительность job (SLI latency на уровне job) |
| `spring_batch_step_seconds{step}` | histogram | с | Длительность шагов - поиск узкого шага (read/validate/enrich/write) |
| `tw_job_active` | gauge | шт | Активные job - контроль конкурентности |
| `tw_job_waiting` | gauge | шт | Ожидающие job - ёмкость |
| `tw_job_restart_total` | counter | шт | Перезапуски |
| `tw_retry_total{step,reason}` | counter | шт | Повторы временных сбоев |
| `tw_skip_total{step,reason}` | counter | шт | Пропущенные записи - качество данных |
| `tw_chunk_commit_total` / `tw_chunk_rollback_total` | counter | шт | Транзакции на chunk, откаты |
| `tw_rows_read_total` / `tw_rows_written_total` | counter | шт | Объём обработки (throughput строк/с) |

### Качество данных

| Метрика | Тип | Ед. | Назначение | Алерт |
|---|---|---|---|---|
| `tw_validation_errors_total{field,code}` | counter | шт | Ошибки валидации по полям | Warning при всплеске |
| `tw_duplicates_total` | counter | шт | Дубликаты строк | Warning при росте |
| `tw_rows_rejected_ratio` | gauge | ratio | Доля отклонённых строк | Critical при > X% |
| `tw_enrich_miss_total` | counter | шт | Не найдено справочных данных | Warning |

### Инфраструктура: JVM и БД

| Метрика | Тип | Ед. | Назначение |
|---|---|---|---|
| `jvm_memory_used_bytes{area}` / `jvm_memory_max_bytes` | gauge | байт | Память JVM worker/API |
| `jvm_gc_pause_seconds` | histogram | с | Паузы GC - влияние на latency |
| `jvm_threads_live_threads` | gauge | шт | Потоки |
| `process_cpu_usage` | gauge | ratio | CPU процесса |
| `hikaricp_connections_active/idle/pending` | gauge | шт | Пул соединений - pending указывает на нехватку |
| `pg_stat_activity_count{state}` | gauge | шт | Соединения БД по состоянию |
| `pg_locks_count{mode}` | gauge | шт | Блокировки - симптом конкуренции |
| `pg_stat_database_deadlocks` | counter | шт | Deadlocks |
| `pg_stat_wal_bytes` | counter | байт | Нагрузка на WAL от batch-записей |
| `pg_stat_database_blks_hit_ratio{db}` | gauge | ratio | Cache hit ratio по каждой БД |
| `pg_stat_activity_count{db,state}` | gauge | шт | Соединения по каждой БД отдельно |
| `tw_refdb_lookup_seconds{op}` | histogram | с | Латентность чтения справочных данных при обогащении (по операциям) |
| `tw_refdb_staleness_seconds` | gauge | с | Возраст последнего обновления справочных данных |

### Файловое хранилище (Google Cloud Storage)

| Метрика | Тип | Ед. | Источник | Назначение | Алерт |
|---|---|---|---|---|---|
| `gcs_bucket_bytes_total{bucket}` | gauge | байт | Cloud Monitoring | Занятый объём бакета отчётов - рост, стоимость | Warning при аномальном росте |
| `gcs_bucket_object_count{bucket}` | gauge | шт | Cloud Monitoring | Число объектов - контроль накопления/"мусора" | - |
| `gcs_object_age_seconds{bucket}` | gauge | с | Расчёт/lifecycle | Возраст самого старого объекта против retention | Warning при превышении TTL |
| `gcs_request_total{bucket,op,status}` | counter | шт | Cloud Monitoring | Число запросов (GET/PUT) и ошибок (4xx/5xx) | Critical при 5xx |
| `gcs_request_latency_seconds{op}` | histogram | с | Cloud Monitoring | Latency операций чтения/записи | Warning при деградации |
| `gcs_network_bytes_total{direction}` | counter | байт | Cloud Monitoring | Egress/ingress - стоимость трафика | - |
| `tw_gcs_upload_seconds` | histogram | с | API/Worker (клиент) | Длительность загрузки/выгрузки файлов | Warning |
| `tw_gcs_upload_total{result}` | counter | шт | API/Worker (клиент) | Успешные/неуспешные операции с хранилищем | Critical при ошибках |

## Почему именно эти метрики

| Симптом из кейса | Метрики, объясняющие/диагностирующие его |
|---|---|
| Отчёты "висят", обновления с задержкой | `tw_report_processing_seconds`, `kafka_consumergroup_lag_seconds`, `kafka_consumergroup_lag`, `spring_batch_step_seconds` |
| Замедляется ERP-интерфейс при загрузке | `http_server_requests_seconds`, `hikaricp_connections_pending`, `process_cpu_usage`, `pg_locks_count` |
| Обновления "не применяются вовсе" | `tw_import_job_total{status}`, `tw_job_restart_total`, `tw_enrich_miss_total` |
| Ошибки/задержки обогащения справочниками | `tw_refdb_lookup_seconds`, `tw_refdb_staleness_seconds`, `pg_stat_database_blks_hit_ratio{db="refdb"}`, `tw_enrich_miss_total` |
| Не загружаются/не выгружаются файлы, рост хранилища | `tw_gcs_upload_total{result}`, `tw_gcs_upload_seconds`, `gcs_request_total{status}`, `gcs_bucket_bytes_total`, `gcs_bucket_object_count` |
| Трудно найти причину ошибки | Корреляция по `jobId`/`traceId` в Loki + `tw_validation_errors_total`, `tw_skip_total`, `tw_retry_total` |
| Пиковый рост 2–3x | `tw_concurrent_imports`, `kafka_consumergroup_lag`, HPA-метрики, `jvm_gc_pause_seconds` |

## SLI/SLO и error budget

| SLI | Определение | SLO (месяц) | Error budget |
|---|---|---|---|
| Latency обработки | Доля отчётов 2000 строк, обработанных ≤30 с (по `tw_report_processing_seconds`) | 99% | 1% |
| Success rate | Доля job без финального FAILED | ≥99.5% | 0.5% |
| Availability API | Доля успешных запросов приёма (не 5xx) | ≥99.9% | 0.1% |

Алерты по error budget: multi-window burn-rate (быстрое окно 5m/1h и медленное 6h/3d).

## Правила алертов (Prometheus → Alertmanager)

| Алерт | Условие | Уровень важности | Маршрут |
|---|---|---|---|
| `QueueBacklogGrowing` | `kafka_consumergroup_lag` выше порога N и растёт 10m | warning | Messenger |
| `QueueOldestMessageStuck` | `kafka_consumergroup_lag_seconds` > T 5m | critical | Messenger |
| `JobFailureRateHigh` | Failure-ratio из `tw_import_job_total` > 0.5% за 15m | critical | Messenger + Email |
| `ReportLatencySLOBreach` | p95 `tw_report_processing_seconds` > 30 с за 30m | critical | Messenger + Email |
| `KafkaUnderReplicated` | `kafka_topic_partition_under_replicated > 0` 5m | critical | Messenger |
| `ValidationErrorSpike` | Всплеск `tw_validation_errors_total` относительно базовой линии | warning | Messenger + Email |
| `DBPoolExhausted` | `hikaricp_connections_pending > 0` 5m | warning | Messenger |
| `DBLockContention` / `DeadlockDetected` | `pg_locks_count` / рост `pg_stat_database_deadlocks` | warning | Messenger |
| `RefDbReadSlow` | p95 `tw_refdb_lookup_seconds` выше порога или падение `pg_stat_database_blks_hit_ratio{db="refdb"}` | warning | Messenger |
| `RefDbStale` | `tw_refdb_staleness_seconds` выше порога (устаревшие справочники/промо) | warning | Messenger + Email |
| `GcsRequestErrorsHigh` | Рост доли 5xx в `gcs_request_total` за 10m | critical | Messenger + Email |
| `GcsUploadFailures` | `increase(tw_gcs_upload_total{result="failed"}[15m]) > 0` | critical | Messenger |
| `StorageGrowthAbnormal` | Аномальный рост `gcs_bucket_bytes_total` относительно базовой линии | warning | Messenger |
| `RetentionBreach` | `gcs_object_age_seconds` > TTL (lifecycle не срабатывает) | warning | Messenger |
| `GCPauseHigh` | p99 `jvm_gc_pause_seconds` > порога | warning | Messenger |

## Требования к логированию (структурированный JSON)

Обязательные поля каждого события: `timestamp`, `level`, `service`, `env`, `jobId`, `fileId`,
`traceId`, `warehouseId`, `step`, `event`, `durationMs`, `rowsProcessed`, `errorCode`, `message`.

Правила: не логировать PII и содержимое строк файла; на ошибку - `errorCode` + контекст без
чувствительных значений; логи в stdout, ретенция и хранение - в Loki.

## Ограничения и дальнейшие шаги

- Трассировка (OpenTelemetry → Tempo) - следующий этап; `traceId` уже присутствует в логах.
- Пороги N/T для алертов уточняются по результатам нагрузочного теста.
