#!/usr/bin/env bash
set -euo pipefail

ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${ALERT_EMAIL:-admin@example.com}"

echo "[start-airflow] Инициализация БД"
airflow db migrate

echo "[start-airflow] Создание администратора ${ADMIN_USERNAME}"
airflow users create \
    --username "${ADMIN_USERNAME}" \
    --password "${ADMIN_PASSWORD}" \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email "${ADMIN_EMAIL}" || true

echo "[start-airflow] Запуск airflow standalone (webserver + scheduler + triggerer)"
exec airflow standalone
