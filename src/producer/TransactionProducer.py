# src/producer/TransactionProducer.py

import json
import os
import random
import signal
import time
import sys # For sys.exit
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException, Consumer # Added Consumer
import logging
from faker import Faker
from dotenv import load_dotenv
from pathlib import Path
from jsonschema import ValidationError, validate, FormatChecker
# Assuming consumer_cache.py is fixed and available
from src.shared.consumer_cache import CacheConsumer # Using the utility class

# --- Setup Logging ---
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
# Try loading .env from project root (assuming structure PROJECT_ROOT/src/producer/...)
# Adjust if your structure is different
GLOBAL_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
if GLOBAL_ENV_PATH.exists() and not os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
     logger.info(f"Loading .env from: {GLOBAL_ENV_PATH}")
     load_dotenv(dotenv_path=GLOBAL_ENV_PATH)
elif not os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
    logger.info("Loading .env from default location.")
    load_dotenv() # Load from current dir or default location

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
# Producer Topic
KAFKA_TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
# Consumer Topics & Group
KAFKA_USERS_TOPIC = os.getenv("KAFKA_USERS_TOPIC", "users")
KAFKA_MERCHANTS_TOPIC = os.getenv("KAFKA_MERCHANTS_TOPIC", "merchants")
CONSUMER_GROUP_ID = os.getenv("TRANSACTION_PRODUCER_CONSUMER_GROUP", "transaction-producer-enrichment-group")
PRODUCER_INTERVAL = float(os.getenv('PRODUCER_INTERVAL', 0.2)) # Potentially faster interval

fake = Faker()
running = True # Global flag for graceful shutdown

# --- Transaction Schema (Ensure it matches expected output) ---
TransactionSchema = {
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
        "is_fraud": {"type": "integer", "enum": [0, 1]} # Include fraud flag if needed
    },
    "required": [
        "transaction_id", "user_id", "amount", "currency",
        "merchant_id", "merchant_name", "merchant_category", "card_type",
        "timestamp", "location_country", "location_city",
        "latitude", "longitude", "is_fraud" # Added is_fraud
    ]
}

MERCHANT_CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]
ISO_CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "INR", "CNY", "ZAR", "AED"]
CARD_TYPES = ["Visa", "MasterCard", "Amex", "Discover", "Other"]

class TransactionProducerWithEnrichment():
    def __init__(self):
        global KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD
        global KAFKA_USERS_TOPIC, KAFKA_MERCHANTS_TOPIC, CONSUMER_GROUP_ID
        global KAFKA_TRANSACTIONS_TOPIC

        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TRANSACTIONS_TOPIC
        self.producer = None
        self.user_cache_consumer = None
        self.merchant_cache_consumer = None

        if not self.bootstrap_servers:
             logger.critical("Missing required environment variable: KAFKA_BOOTSTRAP_SERVERS. Exiting.")
             sys.exit(1)

        # --- Setup Producer ---
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'transaction_producer_{os.getpid()}',
            'compression.type': 'gzip', 'linger.ms': '10', 'batch.size': 32768
        }
        if KAFKA_USERNAME and KAFKA_PASSWORD:
            producer_config.update({
                'security.protocol': 'SASL_PLAINTEXT', 'sasl.mechanism': 'PLAIN',
                'sasl.username': KAFKA_USERNAME, 'sasl.password': KAFKA_PASSWORD
            })
        try:
            self.producer = Producer(producer_config)
            logger.info(f"Kafka producer created for topic '{self.topic}'.")
        except KafkaException as e:
            logger.error(f'Failed to create Kafka producer: {e}', exc_info=True)
            raise e # Propagate

        # --- Setup Consumers/Caches ---
        # Create separate CacheConsumer instances for users and merchants
        try:
            self.user_cache_consumer = CacheConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=CONSUMER_GROUP_ID, # Share group ID or use separate ones? Shared for simplicity now.
                topic=KAFKA_USERS_TOPIC
            )
            logger.info(f"User cache consumer created for topic '{KAFKA_USERS_TOPIC}'.")
        except Exception as e:
             logger.error(f"Failed to create User CacheConsumer: {e}", exc_info=True)
             # Decide if we can proceed without user cache? For now, raise.
             raise e

        try:
            self.merchant_cache_consumer = CacheConsumer(
                 bootstrap_servers=self.bootstrap_servers,
                 group_id=CONSUMER_GROUP_ID, # Shared group ID
                 topic=KAFKA_MERCHANTS_TOPIC
            )
            logger.info(f"Merchant cache consumer created for topic '{KAFKA_MERCHANTS_TOPIC}'.")
        except Exception as e:
              logger.error(f"Failed to create Merchant CacheConsumer: {e}", exc_info=True)
              # Decide if we can proceed without merchant cache? For now, raise.
              raise e

        # --- Fraud Simulation Setup ---
        # Note: compromised_users might be empty initially if cache isn't populated
        self.compromised_users = set() # Will be populated by cache polling or needs alternative setup
        self.high_risk_merchants_names = ['QuickCash', 'GlobalDigital', 'FastMoneyX', fake.company()]

        # Setup signal handling
        signal.signal(signal.SIGINT, self.shutdown_signal)
        signal.signal(signal.SIGTERM, self.shutdown_signal)

    def shutdown_signal(self, signum, frame):
        global running
        if running:
            logger.info(f"Shutdown signal ({signum}) received. Stopping...")
            running = False # Set global flag

    def shutdown_resources(self):
        """Close Kafka clients and other resources."""
        logger.info("Shutting down resources...")
        if self.user_cache_consumer:
            self.user_cache_consumer.close()
        if self.merchant_cache_consumer:
            self.merchant_cache_consumer.close()
        if self.producer:
             logger.info("Flushing final Kafka producer messages...")
             remaining = self.producer.flush(timeout=30)
             if remaining > 0:
                 logger.warning(f"{remaining} transaction messages may not have been delivered.")
             else:
                 logger.info("All transaction messages flushed successfully.")
        logger.info("Resources shut down.")


    def delivery_report(self, err, msg):
        """ Callback for producer results. """
        if err is not None:
            logger.error(f"Transaction message delivery failed: {err}")
        else:
            logger.debug(f"Transaction message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validates a transaction against the schema."""
        try:
            validate(instance=transaction, schema=TransactionSchema, format_checker=FormatChecker())
            return True
        except ValidationError as e:
            logger.error(f"Transaction validation failed: {e.message} for transaction ID: {transaction.get('transaction_id', 'N/A')}")
            return False

    def poll_caches(self):
        """Poll both user and merchant caches to keep them updated."""
        if self.user_cache_consumer:
            self.user_cache_consumer.poll_messages(num_messages=50, timeout=0.1) # Non-blocking poll
        if self.merchant_cache_consumer:
            self.merchant_cache_consumer.poll_messages(num_messages=50, timeout=0.1) # Non-blocking poll

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        """Generates a single enriched transaction using cached user/merchant data."""

        # --- Get User and Merchant from Cache ---
        # This is a critical dependency - might return None if cache is empty
        user_data = self.user_cache_consumer.get_random_user() if self.user_cache_consumer else None
        merchant_data = self.merchant_cache_consumer.get_random_merchant() if self.merchant_cache_consumer else None

        if not user_data or not merchant_data:
            logger.warning("Could not generate transaction: User or Merchant cache is empty/unavailable. Polling caches...")
            self.poll_caches() # Try polling again
            # Maybe wait a bit before next attempt in main loop
            return None # Skip generation this cycle

        user_id = user_data.get("user_id")
        merchant_id = merchant_data.get("merchant_id")
        merchant_name = merchant_data.get("merchant_name", "Unknown") # Default if missing
        merchant_category = merchant_data.get("merchant_category", "Unknown")

        if not user_id or not merchant_id:
             logger.error(f"Invalid data from cache. User: {user_data}, Merchant: {merchant_data}")
             return None

        # --- Generate other transaction details ---
        amount = round(fake.random_number(digits=2, fix_len=False, positive=True) * random.uniform(0.5, 150), 2)
        amount = max(amount, 0.01)
        currency = random.choice(ISO_CURRENCY_CODES)
        card_type = random.choice(CARD_TYPES)
        location_country = fake.country_code()
        location_city = fake.city()
        latitude = round(fake.latitude(), 6)
        longitude = round(fake.longitude(), 6)
        timestamp = datetime.now(timezone.utc).isoformat()

        is_fraud = 0
        # --- Basic Fraud Simulation ---
        # Update compromised users based on cache periodically? Complex. Sticking to initial random set for now.
        if user_id in self.compromised_users and amount > 500 and random.random() < 0.4:
            is_fraud = 1
        elif merchant_name in self.high_risk_merchants_names and amount > 1000 and random.random() < 0.3:
             is_fraud = 1
        # Add other fraud rules as needed...

        # --- Construct Final Transaction ---
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": user_id, # From consumed data
            "amount": amount,
            "currency": currency,
            "merchant_id": merchant_id, # From consumed data
            "merchant_name": merchant_name, # From consumed data
            "merchant_category": merchant_category, # From consumed data
            "card_type": card_type,
            "timestamp": timestamp,
            "location_country": location_country,
            "location_city": location_city,
            "latitude": latitude,
            "longitude": longitude,
            "is_fraud": is_fraud
        }

        # Log if fraud was generated
        # if is_fraud: logger.info(f"Generated FRAUDULENT transaction: {transaction['transaction_id']}")

        return transaction

    def produce_message(self) -> bool:
        """Generates, validates, and produces a single transaction message to Kafka."""
        transaction = self.generate_transaction()

        if not transaction:
            # logger.warning("Failed to generate transaction (likely due to empty caches).")
            return False # Indicate failure for this cycle

        if not self.validate_transaction(transaction):
             logger.error(f"Skipping invalid transaction: {transaction.get('transaction_id', 'N/A')}")
             return False

        try:
            self.producer.produce(
                self.topic,
                key=transaction['transaction_id'].encode('utf-8'), # Use transaction_id as key
                value=json.dumps(transaction).encode('utf-8'),
                callback=self.delivery_report
            )
            return True
        except BufferError:
            logger.warning("Kafka producer buffer is full. Flushing...")
            self.producer.flush(5.0)
            # Retry producing the message
            try:
                self.producer.produce(
                    self.topic, key=transaction['transaction_id'].encode('utf-8'),
                    value=json.dumps(transaction).encode('utf-8'), callback=self.delivery_report
                )
                return True
            except Exception as e:
                logger.error(f"Failed to produce message {transaction['transaction_id']} after buffer full: {e}")
                return False
        except KafkaException as e:
            logger.error(f"KafkaException during produce: {e}")
            # Consider stopping or specific error handling
            global running
            running = False # Stop on persistent Kafka errors
            return False
        except Exception as e:
            logger.error(f"Unexpected error during message production: {e}", exc_info=True)
            return False

    def run_continuous_production(self, interval: float = 0.5):
        """Runs the producer loop, generating messages continuously while polling caches."""
        global running
        logger.info(f"Starting continuous message production for topic '{self.topic}'...")
        logger.info("Also consuming from user/merchant topics for enrichment...")
        message_count = 0
        last_cache_poll_time = time.time()

        while running:
            # --- Poll Caches Periodically ---
            current_time = time.time()
            if current_time - last_cache_poll_time >= 1.0: # Poll caches every second
                 self.poll_caches()
                 last_cache_poll_time = current_time

            # --- Produce Transaction ---
            if self.produce_message():
                message_count += 1

            # --- Poll Producer ---
            # Poll producer non-blockingly to handle delivery reports and network events
            self.producer.poll(0)

            # --- Sleep ---
            try:
                # Sleep only if we are still running
                if running:
                    time.sleep(interval) # Use the configured interval
            except InterruptedError:
                logger.info("Sleep interrupted, likely shutting down.")
                running = False # Ensure loop terminates

        logger.info(f"Exiting production loop. Produced {message_count} messages.")
        # Resource cleanup happens in main's finally block


def main():
    """Entry point for the producer application"""
    producer_instance = None
    try:
        logger.info("Starting Enriched Transaction Producer...")
        producer_instance = TransactionProducerWithEnrichment()
        producer_instance.run_continuous_production(interval=PRODUCER_INTERVAL)
    except KafkaException as e:
         logger.critical(f"Failed to initialize Kafka clients: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Producer application failed unexpectedly: {e}", exc_info=True)
    finally:
        # Ensure resources are cleaned up
        if producer_instance:
            producer_instance.shutdown_resources()
        logger.info("Producer application finished.")


if __name__ == "__main__":
    # Basic check for Kafka env var
    if not os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
         print("ERROR: KAFKA_BOOTSTRAP_SERVERS environment variable not set.", file=sys.stderr)
         sys.exit(1)
    main()