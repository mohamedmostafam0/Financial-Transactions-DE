ARG AIRFLOW_VERSION=3.0.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.9

USER root

RUN mkdir -p /app/src && mkdir -p /app/logs

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# ✅ Use correct relative path
COPY ../src/producer/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

# ✅ Copy full src folder
COPY ../src /opt/airflow/src

RUN echo "Airflow Dockerfile configured successfully."
