import datetime
import json
import os
import random
import signal
import time
from typing import Dict, Optional, Any
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
import logging
from faker import Faker
from jsonschema import ValidationError, validate, FormatChecker
from dotenv import load_dotenv

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
load_dotenv(dotenv_path="/app/.env")

fake = Faker()

TransactionSchema = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "integer", "minimum": 1, "maximum": 10000},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 10000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", "is_fraud"]

}
class TransactionProducer(): 
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME')
        self.kafka_password = os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.running = False

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client_id': 'transaction_producer',
            'compression_type': 'gzip',
            'linger.ms': '5',
            'batch.size': 16384
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                                        'security.protocol': 'SASL_SSL',
                                         'sasl.mechanism': 'PLAIN',
                                       'sasl.username': self.kafka_username,
                                       'sasl.password': self.kafka_password})
        else:
            self.producer_config.update({
                'security.protocol': 'PLAINTEXT'
            })

        try:
            self.producer = Producer(self.producer_config)
            logger.info("Kafka producer created successfully.")
        except Exception as e:
            logger.error(f'Failed to create Kafka producer: {e}')
            raise e
        
        self.compromised_users = set(random.sample(range(500, 5000), 50))
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,
            'card_testing': 0.3,
            'merchant_collusion': 0.2,
            'geo_anomaly': 0.1
        }

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info("Shutting down producer...")
        self.running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Producer shut down successfully.")


    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}") 

    def produce_message(self) -> bool:
        try: 
            transaction = self.generate_transaction()
            logger.info(f"Producing message: {transaction}")
            if not transaction:
                return False
            
            self.producer.produce(self.topic, key=transaction['transaction_id'], value=json.dumps(transaction), callback=self.delivery_report)
            self.producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
            return False
        
    def run_continuous_production(self, interval: float = 0.5):
        self.running = True
        logger.info("Starting continuous message production for topic '%s'...", self.topic)
        try:
            while self.running:
                try:
                    self.produce_message()
                    time.sleep(interval)
                except Exception as e:
                    logger.error(f"Error in message production: {e}")
                    break
        except KeyboardInterrupt:
            logger.info("Continuous production stopped manually.")
            self.running = False

    def generate_transaction(self) -> Optional[Dict[str, any]] :
        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': fake.random_int(min=1, max=10000),
            'amount': round(fake.random_int(min=0.01, max=10000), 2),
            'currency': 'USD',
            'merchant': fake.company(),
            'timestamp': (datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=random.randint(-30, 0))).isoformat(),  
            'location': fake.country_code(),
            'is_fraud': 0                   
        }

        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']
        location = transaction['location']
        timestamp = transaction['timestamp']
        
        #Account takeover
        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3:
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)
                transaction['merchant'] = random.choice(self.high_risk_merchants) 

        #card testing
        if not is_fraud and amount < 2:
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(500, 5000), 2)
                transaction['location'] = 'US'
                transaction['merchant'] = random.choice(self.high_risk_merchants) 

        #merchant collusion
        if not is_fraud and merchant in self.high_risk_merchants and amount > 3000:
            if random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(300, 1500), 2)
                

        #geo anomaly
        # if not is_fraud and location != 'US' and amount > 1000:
        if not is_fraud:
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud = 1
                transaction['location'] = random.choice(['SA', 'CA', 'GB'])

        #baseline random fraud between 0.1% and 0.2%
        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)
            transaction['merchant'] = random.choice(self.high_risk_merchants)
            transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

    def validate_transaction(self, transaction: Dict[str, any]) -> bool:
        try: 
            validate(
                instance=transaction,
                schema=TransactionSchema,
                format_checker=FormatChecker(),

            )
        except ValidationError as e:
            logger.error(f"Transaction validation failed: {e}")
            return False


def main():
    """Entry point for the producer application"""
    try:
        logger.info("Starting transaction producer...")
        producer = TransactionProducer()
        producer.run_continuous_production(interval=0.5)  # 0.5 second between messages
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise


if __name__ == "__main__":
    main()