# src/producers/transaction_producer.py

import json
import time
import signal
import sys
import random
import logging
import uuid
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer, KafkaException
from confluent_kafka.avro import AvroProducer
from jsonschema import validate, ValidationError, FormatChecker
from src.utils.config import KAFKA, SCHEMA_REGISTRY, POSTGRES
from src.utils.schema_registry import SchemaRegistry
from src.utils.postgres_db import PostgresDB

KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_TRANSACTIONS_TOPIC = KAFKA["TOPICS"]["transactions"]
KAFKA_USERS_TOPIC = KAFKA["TOPICS"]["users"]
KAFKA_MERCHANTS_TOPIC = KAFKA["TOPICS"]["merchants"]
CONSUMER_GROUP_ID = KAFKA["GROUPS"]["transaction_producer"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
PRODUCER_INTERVAL = KAFKA["PRODUCER_INTERVAL"]

# PostgreSQL configuration from config.py
PG_ENABLED = POSTGRES["ENABLED"]

MERCHANT_CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]
ISO_CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "INR", "CNY", "ZAR", "AED"]
CARD_TYPES = ["Visa", "MasterCard", "Amex", "Discover", "Other"]

fake = Faker()
running = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
            
        # Initialize schema registry
        self.schema_registry = SchemaRegistry(SCHEMA_REGISTRY["URL"])
        self.transaction_schema = self.schema_registry.get_schema("transaction")

        # Initialize PostgreSQL connection
        self.db = None
        if PG_ENABLED:
            self.db = PostgresDB()
            if self.db.is_connected():
                logger.info("PostgreSQL database initialized and connected")
            else:
                logger.error("PostgreSQL database not available. Cannot generate transactions.")
                sys.exit(1)  # Exit if database connection fails

        # Configure Kafka producer
        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'compression.type': 'gzip',
            'schema.registry.url': SCHEMA_REGISTRY["URL"]
        }

        if KAFKA_USERNAME and KAFKA_PASSWORD:
            sasl_config = {
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': KAFKA_USERNAME,
                'sasl.password': KAFKA_PASSWORD,
            }
            producer_config.update(sasl_config)

        # Create producers
        self.producer = Producer(producer_config)  # Regular producer for fallback
        self.avro_producer = AvroProducer(producer_config)  # Avro producer

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


    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for key {msg.key()}: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


                if msg.topic() == KAFKA_USERS_TOPIC:
                    self.latest_user = data
                elif msg.topic() == KAFKA_MERCHANTS_TOPIC:
                    self.latest_merchant = data
            except Exception as e:
                logger.warning(f"Error parsing message: {e}")

    def _generate_transaction(self):
        # Only use PostgreSQL database for user and merchant data
        if not self.db or not self.db.is_connected():
            logger.error("PostgreSQL database not connected. Cannot generate transaction.")
            return None
            
        # Get a random user from database
        user = self.db.get_random_user()
        if not user:
            logger.warning("No users available in database. Cannot generate transaction.")
            return None
        
        # Get a random merchant from database
        merchant = self.db.get_random_merchant()
        if not merchant:
            logger.warning("No merchants available in database. Cannot generate transaction.")
            return None
            
        logger.info(f"Retrieved data from PostgreSQL - User: {user['user_id']}, Merchant: {merchant['merchant_id']}")

        
        txn = {
            "transaction_id": fake.uuid4(),
            "user_id": user["user_id"],
            "amount": float(round(random.uniform(5, 5000), 2)),
            "currency": random.choice(ISO_CURRENCY_CODES),
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": merchant.get("merchant_category", "Unknown"),
            "card_type": random.choice(CARD_TYPES),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "location_country": fake.country_code(),
            "location_city": fake.city(),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude())
        }
        
        # Transactions are only published to Kafka, not stored in PostgreSQL
        return txn if self._validate_transaction(txn) else None

    def run(self):
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logger.info(f"Transaction producer started for topic: {KAFKA_TRANSACTIONS_TOPIC}")
        
        # Make sure PostgreSQL is connected before generating transactions
        if not self.db or not self.db.is_connected():
            logger.error("PostgreSQL database not available. Cannot generate transactions.")
            sys.exit(1)
        
        logger.info("Using PostgreSQL database for user and merchant data")
        
        transaction_count = 0
        
        while running:
            txn = self._generate_transaction()
            if txn:
                try:
                    # Use Avro serialization
                    self.avro_producer.produce(
                        topic=KAFKA_TRANSACTIONS_TOPIC,
                        key=str(txn['transaction_id']),
                        value=txn,
                        value_schema=json.dumps(self.transaction_schema)
                    )
                    logger.info(f"Produced transaction with Avro: {txn['transaction_id']}")
                    transaction_count += 1
                    
                    # Log summary every 100 transactions
                    if transaction_count % 100 == 0:
                        logger.info(f"Produced {transaction_count} transactions so far")
                        
                except BufferError:
                    logger.warning("Producer buffer full, flushing...")
                    self.avro_producer.flush(5)
                    # Retry after flush
                    try:
                        self.avro_producer.produce(
                            topic=KAFKA_TRANSACTIONS_TOPIC,
                            key=str(txn['transaction_id']),
                            value=txn,
                            value_schema=json.dumps(self.transaction_schema)
                        )
                        transaction_count += 1
                    except Exception as e:
                        logger.error(f"Retry failed: {e}")
                        # Fallback to regular producer
                        try:
                            self.producer.produce(
                                KAFKA_TRANSACTIONS_TOPIC,
                                key=str(txn['transaction_id']),
                                value=json.dumps(txn).encode('utf-8'),
                                callback=self.delivery_report
                            )
                            logger.info(f"Produced transaction with JSON fallback: {txn['transaction_id']}")
                            transaction_count += 1
                        except Exception as e:
                            logger.error(f"Fallback also failed: {e}")
                except Exception as e:
                    logger.error(f"Failed to produce message with Avro: {e}")
                    # Fallback to regular producer
                    try:
                        self.producer.produce(
                            KAFKA_TRANSACTIONS_TOPIC,
                            key=str(txn['transaction_id']),
                            value=json.dumps(txn).encode('utf-8'),
                            callback=self.delivery_report
                        )
                        logger.info(f"Produced transaction with JSON fallback: {txn['transaction_id']}")
                        transaction_count += 1
                    except Exception as e:
                        logger.error(f"Fallback also failed: {e}")

            self.avro_producer.poll(0)
            time.sleep(PRODUCER_INTERVAL)

        logger.info(f"Shutting down producer after generating {transaction_count} transactions...")
        if hasattr(self, 'consumer') and self.consumer:
            self.consumer.close()
        self.avro_producer.flush()
        self.producer.flush()
        logger.info("TransactionProducer finished.")

if __name__ == "__main__":
    TransactionProducer().run()
