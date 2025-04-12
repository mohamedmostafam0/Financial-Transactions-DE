import requests
import json
from datetime import datetime
from minio import Minio
from minio.error import S3Error

# === Configuration ===
APP_ID = "YOUR_APP_ID"  # Replace with your Open Exchange Rates API key
BASE_CURRENCY = "USD"
MINIO_ENABLED = True

MINIO_CONFIG = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket_name": "currency-data",
    "object_name_prefix": "exchange-rates/"
}


def fetch_exchange_rates(app_id: str, base_currency: str = "USD"):
    url = f"https://openexchangerates.org/api/latest.json?app_id={app_id}&base={base_currency}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(f"[✓] Fetched exchange rates: {len(data['rates'])} currencies")
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")


def save_to_json(data: dict, filename: str):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[✓] Saved exchange rates to {filename}")


def upload_to_minio(filename: str, config: dict):
    client = Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=False
    )

    # Create bucket if not exists
    if not client.bucket_exists(config["bucket_name"]):
        client.make_bucket(config["bucket_name"])
        print(f"[+] Created bucket '{config['bucket_name']}'")

    # Upload object
    object_name = f"{config['object_name_prefix']}rates_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.json"
    client.fput_object(
        config["bucket_name"], object_name, filename
    )
    print(f"[✓] Uploaded '{filename}' to MinIO as '{object_name}'")


if __name__ == "__main__":
    filename = f"exchange_rates_{datetime.now().strftime('%Y%m%d')}.json"
    data = fetch_exchange_rates(APP_ID, BASE_CURRENCY)
    save_to_json(data, filename)

    if MINIO_ENABLED:
        upload_to_minio(filename, MINIO_CONFIG)
