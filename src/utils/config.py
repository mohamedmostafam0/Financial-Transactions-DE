import os
from dotenv import load_dotenv

# Automatically load environment variables from .env
load_dotenv()

# === Kafka (Confluent Cloud) Configuration ===
KAFKA = {
    "BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "USERNAME": os.getenv("KAFKA_USERNAME"),
    "PASSWORD": os.getenv("KAFKA_PASSWORD"),
    "PRODUCER_INTERVAL": float(os.getenv("PRODUCER_INTERVAL")),
    "TOPICS": {
        "transactions": os.getenv("KAFKA_TRANSACTIONS_TOPIC"),
        "users": os.getenv("KAFKA_USERS_TOPIC"),
        "merchants": os.getenv("KAFKA_MERCHANTS_TOPIC"),
        "blacklist": os.getenv("KAFKA_BLACKLIST_TOPIC"),
        "exchange_rates": os.getenv("KAFKA_EXCHANGE_RATES_TOPIC", "exchange_rates")
    },
    "GROUPS": {
        "blacklist": os.getenv("BLACKLIST_CONSUMER_GROUP"),
        "transaction_producer": os.getenv("TRANSACTION_PRODUCER_CONSUMER_GROUP")
    }
}

# === Blacklist Logic ===
BLACKLIST_PROBABILITY = float(os.getenv("BLACKLIST_PROBABILITY", "0.01"))

# === Exchange Rate API ===
EXCHANGE_RATE_API = {
    "APP_ID": os.getenv("OPENEXCHANGERATES_APP_ID"),
    "BASE_CURRENCY": os.getenv("BASE_CURRENCY", "USD")
}

# === Google Cloud / BigQuery Configuration ===
BIGQUERY = {
    "PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
    "DATASET": os.getenv("BQ_DATASET"),
    "TABLE": os.getenv("BQ_TABLE"),
    "GCS_URI": os.getenv("GCS_URI"),
    "PARQUET_OUTPUT_BUCKET": os.getenv("PARQUET_OUTPUT_BUCKET"),
    "PARQUET_OUTPUT_PREFIX": os.getenv("PARQUET_OUTPUT_PREFIX")
}

# === Optional: Postgres / Airflow config (if needed in DAGs) ===
AIRFLOW = {
    "USER": os.getenv("AIRFLOW_USER"),
    "PASSWORD": os.getenv("AIRFLOW_PASSWORD"),
    "FERNET_KEY": os.getenv("AIRFLOW_FERNET_KEY")
}

# === Schema Registry Configuration ===
SCHEMA_REGISTRY = {
    "URL": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    "SCHEMAS": {
        "transactions": "transaction",
        "users": "user",
        "merchants": "merchant",
        "blacklist": "blacklist",
        "exchange_rates": "exchange_rate"
    }
}

# === PostgreSQL Configuration ===
POSTGRES = {
    "ENABLED": os.getenv("PG_ENABLED", "true").lower() == "true",
    "HOST": os.getenv("PG_HOST", "localhost"),
    "PORT": os.getenv("PG_PORT", "5432"),
    "DATABASE": os.getenv("PG_DATABASE", "financial_transactions"),
    "USER": os.getenv("PG_USER", "finapp"),
    "PASSWORD": os.getenv("PG_PASSWORD", "finapp123")
}
