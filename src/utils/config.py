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

        "exchange_rates": os.getenv("KAFKA_EXCHANGE_RATES_TOPIC", "exchange_rates"),
        "economic_indicators": os.getenv("KAFKA_ECONOMIC_INDICATORS_TOPIC", "economic_indicators")
    },
    "GROUPS": {

        "transaction_producer": os.getenv("TRANSACTION_PRODUCER_CONSUMER_GROUP")
    }
}



# === Exchange Rate API ===
EXCHANGE_RATE_API = {
    "APP_ID": os.getenv("OPENEXCHANGERATES_APP_ID"),
    "BASE_CURRENCY": os.getenv("BASE_CURRENCY", "USD")
}

# === Economic Indicators API (FRED) ===
ECONOMIC_API = {
    "FRED_API_KEY": os.getenv("FRED_API_KEY", "0a3e985f10a3fcf47316915cf417e30a"),
    "UPDATE_INTERVAL": int(os.getenv("ECONOMIC_UPDATE_INTERVAL", "86400"))  # Default: daily
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

# === Airflow Configuration ===
AIRFLOW = {
    "CONNECTIONS": {
        "default": {
            "conn_type": "http",
            "host": "localhost",
            "port": "8080"
        }
    }
}

# === Database Configuration ===
DATABASE = {
    "bigquery": {
        "project": os.getenv("GCP_PROJECT_ID", "your-project-id"),
        "dataset": os.getenv("BQ_DATASET", "financial_transactions")
    }
}

# === Schema Registry Configuration ===
SCHEMA_REGISTRY = {
    "URL": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    "SCHEMAS": {
        "transactions": "transaction",
        "users": "user",
        "merchants": "merchant",

        "exchange_rates": "exchange_rate",
        "economic_indicators": "economic_indicator"
    }
}
