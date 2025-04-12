# --- merchant_producer.py ---
import json
import os
import time
import uuid
import random
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
})

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

