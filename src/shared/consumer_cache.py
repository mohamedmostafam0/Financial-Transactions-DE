import json
import os
from confluent_kafka import Consumer

class CacheConsumer:
    def __init__(self, topic, group_id):
        self.topic = topic
        self.cache = {}

        self.consumer = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.topic])

    def poll_cache(self):
        msg = self.consumer.poll(1.0)
        if msg is None or msg.error():
            return
        data = json.loads(msg.value().decode('utf-8'))
        key = data.get("merchant_id") or data.get("user_id")
        self.cache[key] = data

    def get_all(self):
        for _ in range(50):
            self.poll_cache()
        return list(self.cache.values())
