# Use the same base image version as specified in docker-compose.yaml
# ARG AIRFLOW_VERSION=2.10.5
# FROM apache/airflow:${AIRFLOW_VERSION}-python3.9

USER root
RUN mkdir -p /app/models
RUN echo "DOCKERFILE WAS READ SUCCESSFULLY" 
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* 

# Set Python 3.9 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1 && \
    update-alternatives --set python /usr/bin/python3.9 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 && \
    update-alternatives --set python3 /usr/bin/python3.9

# Switch to airflow user for Python package installation
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN python3.9 -m pip install --upgrade pip && \
    python3.9 -m pip install -r /tmp/requirements.txt
RUN echo "tany part"

USER airflow
# Ensure the installation is in Python path
ENV PYTHONPATH=/home/airflow/.local/lib/python3.9/site-packages:${PYTHONPATH}