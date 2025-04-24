# Dockerfile for Airflow tasks (runs FraudBlacklist, producers, fetch_exchange_rate, etc.)
ARG AIRFLOW_VERSION=3.0.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.9

USER root

# Optional: Create directory for custom scripts/logs/models
RUN mkdir -p /app/src && \
    mkdir -p /app/logs

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Switch to airflow user to install Python requirements
USER airflow

# Copy and install Python requirements (if using any extra libs for producers)
COPY ./src/producer/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

# Copy the application code
COPY ./src /opt/airflow/src

# Set environment path for custom code
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"

# Optional: confirm Dockerfile was loaded
RUN echo "Airflow Dockerfile configured successfully."
