FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    libssl-dev \
    libsasl2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY ./src/producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt


# Copy producer code and supervisord config
# Ensure these paths are correct relative to the build context (project root)
COPY ./src/producer/TransactionProducer.py /app/TransactionProducer.py
COPY ./src/producer/user_producer.py /app/user_producer.py
COPY ./src/producer/merchant_producer.py /app/merchant_producer.py
COPY ./src/producer/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy the __init__.py files if they exist and are needed for imports
COPY ./src/producer/__init__.py /app/src/producer/__init__.py
COPY ./src/shared/__init__.py /app/src/shared/__init__.py


COPY . .

# Run supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
