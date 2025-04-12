import json
import os
import time
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
})

def generate_user():
    return {
        "user_id": fake.random_int(min=1, max=10000),
        "name": fake.name(),
        "email": fake.email()
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def run():
    topic = os.getenv('KAFKA_USER_TOPIC', 'users')
    while True:
        user = generate_user()
        producer.produce(
            topic,
            key=str(user["user_id"]),
            value=json.dumps(user),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    run()