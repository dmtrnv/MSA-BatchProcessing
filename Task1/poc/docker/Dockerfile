FROM apache/airflow:2.10.5-python3.11

USER root

# Java требуется для PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY start-airflow.sh /start-airflow.sh
