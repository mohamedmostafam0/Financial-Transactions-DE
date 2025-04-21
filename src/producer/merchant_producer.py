# --- merchant_producer.py ---
import json
import os
import time
import uuid
import random
from faker import Faker
from confluent_kafka import Producer
from src.config import KAFKA
fake = Faker()

CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]

# Extract config values
KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
KAFKA_USERS_TOPIC = KAFKA["TOPICS"]["users"]
PRODUCER_INTERVAL = KAFKA.get("PRODUCER_INTERVAL", 1)

# Build Kafka producer config
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'merchant-producer',
    'compression.type': 'gzip',
    'linger.ms': 100,
}

# Add secure settings if using Confluent Cloud
if KAFKA_USERNAME and KAFKA_PASSWORD:
    producer_config.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
    })

# Initialize Kafka Producer
producer = Producer(producer_config)

def generate_merchant():
    return {
        "merchant_id": str(uuid.uuid4()),
        "merchant_name": fake.company(),
        "merchant_category": random.choice(CATEGORIES)
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def run():
    topic = os.getenv('KAFKA_MERCHANT_TOPIC', 'merchants')
    while True:
        merchant = generate_merchant()
        producer.produce(
            topic,
            key=merchant["merchant_id"],
            value=json.dumps(merchant),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    run()

