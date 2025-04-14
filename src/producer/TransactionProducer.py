# src/producer/TransactionProducer.py

import os
import json
import time
import signal
import sys
import random
import logging
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer, Consumer, KafkaException
from jsonschema import validate, ValidationError, FormatChecker

# --- Config ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
KAFKA_USERS_TOPIC = os.getenv("KAFKA_USERS_TOPIC", "users")
KAFKA_MERCHANTS_TOPIC = os.getenv("KAFKA_MERCHANTS_TOPIC", "merchants")
CONSUMER_GROUP_ID = os.getenv("TRANSACTION_PRODUCER_CONSUMER_GROUP", "transaction-producer-enrichment-group")
PRODUCER_INTERVAL = float(os.getenv("PRODUCER_INTERVAL", 0.5))

KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

MERCHANT_CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]
ISO_CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "INR", "CNY", "ZAR", "AED"]
CARD_TYPES = ["Visa", "MasterCard", "Amex", "Discover", "Other"]

fake = Faker()
running = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TransactionProducer")

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string", "format": "uuid"},
        "user_id": {"type": "integer", "minimum": 1},
        "amount": {"type": "number", "minimum": 0.01},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant_id": {"type": "string", "format": "uuid"},
        "merchant_name": {"type": "string"},
        "merchant_category": {"type": "string"},
        "card_type": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location_country": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "location_city": {"type": "string"},
        "latitude": {"type": "number", "minimum": -90, "maximum": 90},
        "longitude": {"type": "number", "minimum": -180, "maximum": 180},
    },
    "required": [
        "transaction_id", "user_id", "amount", "currency",
        "merchant_id", "merchant_name", "merchant_category", "card_type",
        "timestamp", "location_country", "location_city",
        "latitude", "longitude"
    ]
}

class TransactionProducer:
    def __init__(self):
        if not KAFKA_BOOTSTRAP_SERVERS:
            logger.critical("KAFKA_BOOTSTRAP_SERVERS not set.")
            sys.exit(1)

        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'compression.type': 'gzip',
        }
        consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
        }

        if KAFKA_USERNAME and KAFKA_PASSWORD:
            sasl_config = {
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': KAFKA_USERNAME,
                'sasl.password': KAFKA_PASSWORD,
            }
            producer_config.update(sasl_config)
            consumer_config.update(sasl_config)

        self.producer = Producer(producer_config)
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([KAFKA_USERS_TOPIC, KAFKA_MERCHANTS_TOPIC])

        self.latest_user = None
        self.latest_merchant = None

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        global running
        logger.info("Shutdown signal received.")
        running = False

    def _validate_transaction(self, transaction):
        try:
            validate(transaction, TRANSACTION_SCHEMA, format_checker=FormatChecker())
            return True
        except ValidationError as e:
            logger.warning(f"Validation failed: {e.message}")
            return False

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for key {msg.key()}: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


    def _consume_until_ready(self):
        logger.info("Waiting to receive at least one user and one merchant...")
        while running and (self.latest_user is None or self.latest_merchant is None):
            msg = self.consumer.poll(1.0)
            if msg and msg.value():
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    if msg.topic() == KAFKA_USERS_TOPIC:
                        self.latest_user = data
                        logger.info("Received initial user data.")
                    elif msg.topic() == KAFKA_MERCHANTS_TOPIC:
                        self.latest_merchant = data
                        logger.info("Received initial merchant data.")
                except Exception as e:
                    logger.warning(f"Error parsing message during warm-up: {e}")

    def _consume_latest(self):
        msg = self.consumer.poll(0.1)
        if msg and msg.value():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                if msg.topic() == KAFKA_USERS_TOPIC:
                    self.latest_user = data
                elif msg.topic() == KAFKA_MERCHANTS_TOPIC:
                    self.latest_merchant = data
            except Exception as e:
                logger.warning(f"Error parsing message: {e}")

    def _generate_transaction(self):
        if not self.latest_user or not self.latest_merchant:
            logger.warning("Latest user or merchant data not available.")
            return None
        
        txn = {
            "transaction_id": fake.uuid4(),
            "user_id": self.latest_user["user_id"],
            "amount": round(random.uniform(5, 500), 2),
            "currency": random.choice(ISO_CURRENCY_CODES),
            "merchant_id": self.latest_merchant["merchant_id"],
            "merchant_name": self.latest_merchant["merchant_name"],
            "merchant_category": self.latest_merchant.get("merchant_category", "Unknown"),
            "card_type": random.choice(CARD_TYPES),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "location_country": fake.country_code(),
            "location_city": fake.city(),
            "latitude": fake.latitude(),
            "longitude": fake.longitude()
        }
        return txn if self._validate_transaction(txn) else None

    def run(self):
        logger.info("TransactionProducer started.")
        self._consume_until_ready()

        while running:
            self._consume_latest()

            txn = self._generate_transaction()
            if txn:
                try:
                    self.producer.produce(
                        KAFKA_TRANSACTIONS_TOPIC,
                        key=str(txn['transaction_id']),
                        value=json.dumps(txn).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    logger.info(f"Produced transaction: {txn['transaction_id']}")
                except BufferError:
                    logger.warning("Producer buffer full, flushing...")
                    self.producer.flush(5)
                except Exception as e:
                    logger.error(f"Failed to produce message: {e}")

            self.producer.poll(0)
            time.sleep(PRODUCER_INTERVAL)

        logger.info("Shutting down producer...")
        self.consumer.close()
        self.producer.flush()
        logger.info("TransactionProducer finished.")

if __name__ == "__main__":
    TransactionProducer().run()
