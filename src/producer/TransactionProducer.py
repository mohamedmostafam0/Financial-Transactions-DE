# producer/main.py
import json
import os
import random
import signal
import time
from typing import Dict, Any
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException
import logging
from faker import Faker
from jsonschema import ValidationError, validate, FormatChecker
from dotenv import load_dotenv # Keep for local testing if needed, but rely on env_file in Docker
from src.shared.consumer_cache import CacheConsumer
from src.producer.merchant_producer import MERCHANTS # Assuming this is a list of dictionaries with merchant data

cache = CacheConsumer(KAFKA_BOOTSTRAP_SERVERS)
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load .env only if running locally without Docker's env_file
if not os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
    load_dotenv() # Assumes .env is in the same directory or parent

fake = Faker()

TransactionSchema = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "integer"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},

        "merchant_id": {"type": "string"},
        "merchant_name": {"type": "string"},
        "merchant_category": {"type": "string"},
        "card_type": {"type": "string"},
        "timestamp": {"type": "string"},
        "location_country": {"type": "string"},
        "location_city": {"type": "string"},
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
    },
    "required": [
        "transaction_id",
        "user_id",
        "amount",
        "currency",
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "card_type",
        "timestamp",
        "location_country",
        "location_city",
        "latitude",
        "longitude",
    ]
}


class TransactionProducer():
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME') # Optional, for SASL
        self.kafka_password = os.getenv('KAFKA_PASSWORD') # Optional, for SASL
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.running = False
        self.producer = None # Initialize producer as None

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'transaction_producer_{os.getpid()}', # Unique client.id per instance
            'compression.type': 'gzip',
            'linger.ms': '10', # Slightly longer linger for better batching
            'batch.size': 32768 # Larger batch size
        }

        # else: # Default to PLAINTEXT if no credentials - No need for explicit else
        #     self.producer_config.update({'security.protocol': 'PLAINTEXT'})
        #     logger.info("Configuring Kafka Producer with PLAINTEXT.")

        try:
            self.producer = Producer(self.producer_config)
            logger.info(f"Kafka producer created successfully for servers {self.bootstrap_servers}.")
        except KafkaException as e:
            logger.error(f'Failed to create Kafka producer: {e}')
            raise e # Propagate the error to prevent startup if connection fails

        self.compromised_users = set(random.sample(range(500, 5000), 50))
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX', fake.company(), fake.company()] # Add some dynamic ones
        # Removed fraud_pattern_weights as they weren't used

        # Setup signal handling
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown) # Handle termination signal from Docker

    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info(f"Shutdown signal ({signum}) received. Stopping producer...")
            self.running = False
            # No need to explicitly close producer here, flush handles pending messages
            # Let the main loop exit and handle flushing there

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validates a transaction against the schema."""
        try:
            validate(
                instance=transaction,
                schema=TransactionSchema,
                format_checker=FormatChecker(), # Enable format validation (date-time, uuid)
            )
            return True
        except ValidationError as e:
            logger.error(f"Transaction validation failed: {e.message} for transaction: {transaction}")
            return False

    # Common ISO currency codes for realism

    def generate_transaction() -> Dict[str, Any]:
        
        ISO_CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "INR", "CNY", "ZAR", "AED"]
        
        """Generates a single fake financial transaction, including currency."""
        user_id = fake.random_int(min=1, max=10000)
        amount = round(fake.random_number(digits=2, fix_len=False, positive=True) * random.uniform(0.5, 100), 2)
        amount = max(amount, 0.01)  # Ensure minimum amount

        currency = random.choice(ISO_CURRENCY_CODES)

        merchant_id = fake.uuid4()
        merchant_name = fake.company()
        merchant_category = fake.random_element(elements=("Retail", "Electronics", "Travel", "Dining", "Services"))

        card_type = random.choice(["Visa", "MasterCard", "Amex", "Discover", "Other"])
        location_country = fake.country_code()
        location_city = fake.city()
        latitude = round(fake.latitude(), 6)
        longitude = round(fake.longitude(), 6)
        timestamp = datetime.now(timezone.utc).isoformat()

        # In generate_transaction
        merchant = random.choice(MERCHANTS)

        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": user_id,
            "amount": amount,
            "currency": currency,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": merchant["merchant_category"],
            "card_type": card_type,
            "timestamp": timestamp,
            "location_country": location_country,
            "location_city": location_city,
            "latitude": latitude,
            "longitude": longitude,
        }

        return transaction

    def produce_message(self) -> bool:
        """Generates, validates, and produces a single transaction message."""
        try:
            transaction = self.generate_transaction()
            if not transaction:
                logger.warning("Failed to generate transaction.")
                return False

            # Validate the transaction before sending
            if not self.validate_transaction(transaction):
                 # Decide whether to skip or send to a dead-letter queue
                 logger.error(f"Skipping invalid transaction: {transaction.get('transaction_id', 'N/A')}")
                 return False # Skip invalid message

            # Produce the message
            self.producer.produce(
                self.topic,
                key=transaction['transaction_id'], # Use transaction_id as key for partitioning
                value=json.dumps(transaction),
                callback=self.delivery_report
            )
            # producer.poll() serves delivery reports and callbacks
            # Call it periodically, not necessarily after every produce() for performance
            # It's called in the run loop.
            return True

        except BufferError:
            logger.warning("Kafka producer queue is full. Pausing slightly.")
            self.producer.poll(1) # Poll to clear buffer/handle callbacks
            return False # Indicate failure to produce this cycle
        except KafkaException as e:
            logger.error(f"Failed to produce message due to Kafka error: {e}")
            # Potentially add logic here to attempt reconnection or shutdown
            self.running = False # Stop production on critical Kafka error
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during message production: {e}", exc_info=True)
            return False

    def run_continuous_production(self, interval: float = 0.5):
        """Runs the producer loop, generating messages continuously."""
        self.running = True
        logger.info(f"Starting continuous message production for topic '{self.topic}'...")
        message_count = 0
        while self.running:
            if self.produce_message():
                message_count += 1
            # Efficiently poll for delivery reports without blocking production much
            self.producer.poll(0)
            try:
                # Sleep only if we are still running
                if self.running:
                    time.sleep(interval)
            except InterruptedException:
                # Handle potential interrupt during sleep if necessary
                self.running = False

        logger.info(f"Exiting production loop. Produced {message_count} messages.")
        logger.info("Flushing final messages...")
        try:
            remaining = self.producer.flush(timeout=10) # Wait up to 10s for messages to send
            if remaining > 0:
                logger.warning(f"{remaining} messages may not have been delivered after flush.")
            else:
                logger.info("All messages flushed successfully.")
        except Exception as e:
            logger.error(f"Error during producer flush: {e}")


    def health_check(self):
        """Basic health check for Docker."""
        # More robust: check if producer object exists and maybe try list_topics (requires admin client)
        if self.producer:
            # Minimal check: see if the producer object exists
            logger.info("Health check: Producer object exists.")
            # A more involved check could try a non-blocking metadata request
            # but might be overkill for a simple health check.
            return True # Or exit(0) if used in CMD-SHELL
        else:
            logger.error("Health check failed: Producer object not initialized.")
            return False # Or exit(1)

def main():
    """Entry point for the producer application"""
    producer_instance = None
    try:
        logger.info("Starting transaction producer...")
        producer_instance = TransactionProducer()
        producer_instance.run_continuous_production(interval=float(os.getenv('PRODUCER_INTERVAL', 0.5)))
    except KafkaException as e:
         logger.critical(f"Failed to initialize Kafka producer, cannot start: {e}")
         # No producer_instance to cleanup here
    except Exception as e:
        logger.error(f"Producer application failed unexpectedly: {e}", exc_info=True)
    finally:
        # Shutdown sequence is handled by signal handlers and the end of run_continuous_production
        logger.info("Producer application finished.")


if __name__ == "__main__":
    main()