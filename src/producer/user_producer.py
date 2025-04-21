import json
import os
import time
from faker import Faker
from confluent_kafka import Producer
from src.config import KAFKA

fake = Faker()

# Extract config values
KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
KAFKA_USERS_TOPIC = KAFKA["TOPICS"]["users"]
PRODUCER_INTERVAL = KAFKA.get("PRODUCER_INTERVAL", 1)

# Build Kafka producer config
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'user-producer',
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

# Generate user messages
def generate_user():
    return {
        "user_id": fake.random_int(min=1, max=10000),
        "name": fake.name(),
        "email": fake.email()
    }

# Delivery callback
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# Main loop
def run():
    print(f"🚀 User producer started for topic: {KAFKA_USERS_TOPIC}")
    while True:
        user = generate_user()
        try:
            producer.produce(
                topic=KAFKA_USERS_TOPIC,
                key=str(user["user_id"]),
                value=json.dumps(user),
                callback=delivery_report
            )
        except BufferError:
            print("⚠️ Buffer full. Flushing and retrying...")
            producer.flush(5)
            continue
        except Exception as e:
            print(f"❗ Error producing message: {e}")

        producer.poll(0)
        time.sleep(PRODUCER_INTERVAL)

if __name__ == "__main__":
    run()
