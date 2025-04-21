# Rename this file? e.g., src/processors/blacklist_processor.py or similar
# It now processes user/merchant streams to produce blacklist events.

import uuid
import random
import json
import os
import logging
from datetime import datetime, timezone
import sys
import signal # For graceful shutdown
from faker import Faker
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from src.config import KAFKA, BLACKLIST_PROBABILITY

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERS_TOPIC = KAFKA["TOPICS"]["users"]
KAFKA_MERCHANTS_TOPIC = KAFKA["TOPICS"]["merchants"]
KAFKA_BLACKLIST_TOPIC = KAFKA["TOPICS"]["blacklist"]
CONSUMER_GROUP_ID = KAFKA["GROUPS"]["blacklist"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]

fake = Faker()
running = True # Global flag for graceful shutdown

# --- Kafka Client Setup --- (Can be refactored to shared utils)
def create_kafka_producer():
    """Creates and returns a Kafka Producer instance."""
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'fraud-blacklist-producer-instance',
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        producer_config.update({
            'security.protocol': 'SASL_SSL', # Or SASL_SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        })
    try:
        return Producer(producer_config)
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}", exc_info=True)
        sys.exit(1)

def create_kafka_consumer(group_id: str, topics: list[str]):
    """Creates and returns a Kafka Consumer instance subscribed to topics."""
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True, # Auto commit offsets
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 10000,
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
         consumer_config.update({
            'security.protocol': 'SASL_SSL', # Or SASL_SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        })
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        logger.info(f"Kafka Consumer subscribed to topics: {topics} with group '{group_id}'")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}", exc_info=True)
        sys.exit(1)

def delivery_report(err, msg):
    """ Callback for Kafka Produce results. """
    if err is not None:
        logger.error(f"Blacklist message delivery failed for key {msg.key()}: {err}")
    else:
        logger.debug(f"Blacklist message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} | Key: {msg.key()}")

# --- Topic Creation --- (Can be refactored)
def create_topic_if_not_exists(bootstrap_servers, topic_name):
    """Creates the Kafka topic if it doesn't exist."""
    admin_config = {'bootstrap.servers': bootstrap_servers}
    # Add SASL if needed
    if KAFKA_USERNAME and KAFKA_PASSWORD:
         admin_config.update({
            'security.protocol': 'SASL_SSL', # Or SASL_SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        })
    try:
        admin_client = AdminClient(admin_config)
        topic_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in topic_metadata.topics:
            logger.info(f"Topic '{topic_name}' not found. Attempting to create...")
            new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
            futures = admin_client.create_topics([new_topic])
            futures[topic_name].result()
            logger.info(f"Topic '{topic_name}' created successfully.")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
         logger.error(f"Failed to check/create topic '{topic_name}': {e}. Continuing...")


# --- Signal Handling ---
def shutdown_handler(signum, frame):
    global running
    logger.info(f"Shutdown signal ({signum}) received. Stopping...")
    running = False

# --- Main Processing Logic ---
def process_message(msg, producer, output_topic):
    """Processes a consumed message and potentially publishes a blacklist entry."""
    global BLACKLIST_PROBABILITY
    if msg is None:
        return
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event - not an error
            logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
        else:
            logger.error(f"Kafka Consume error: {msg.error()}")
        return

    try:
        topic = msg.topic()
        value = msg.value().decode('utf-8')
        data = json.loads(value)
        logger.debug(f"Consumed message from topic '{topic}': {data}")

        entry = None
        message_key = None
        entry_type = None
        entity_id = None

        # Decide whether to blacklist based on probability
        if random.random() < BLACKLIST_PROBABILITY:
            blacklist_id = str(uuid.uuid4())
            common_data = {
                 "blacklist_id": blacklist_id,
                 "reason": fake.sentence(nb_words=random.randint(4, 10)),
                 "flagged_at": datetime.now(timezone.utc).isoformat()
            }

            if topic == KAFKA_USERS_TOPIC and "user_id" in data:
                entity_id = data["user_id"]
                entry_type = "user"
                entry = {**common_data, "entry_type": entry_type, "user_id": entity_id, "merchant_id": None}
                message_key = str(entity_id)
                
            elif topic == KAFKA_MERCHANTS_TOPIC and "merchant_id" in data:
                entity_id = data["merchant_id"]
                entry_type = "merchant"
                entry = {**common_data, "entry_type": "merchant", "user_id": None, "merchant_id": entity_id}
                message_key = entity_id
            else:
                 logger.warning(f"Message from topic '{topic}' missing expected ID field.")
                 return # Cannot blacklist without ID

            # Publish the blacklist entry
            if entry and message_key:
                try:
                    producer.produce(
                        topic=output_topic,
                        key=message_key.encode('utf-8'),
                        value=json.dumps(entry).encode('utf-8'),
                        callback=delivery_report
                    )
                    logger.info(f"Published blacklist entry for {entry_type} ID {entity_id}")
                    producer.poll(0) # Poll frequently during production
                    
                except BufferError:
                    logger.warning("Kafka producer queue full. Flushing...")
                    producer.flush(5.0)
                    # Retry after flush
                    producer.produce(
                         topic=output_topic,
                         key=message_key.encode('utf-8'),
                         value=json.dumps(entry).encode('utf-8'),
                         callback=delivery_report
                    )
                except Exception as e:
                    logger.error(f"Failed to produce blacklist message for {entry_type} ID {entity_id}: {e}")
        # else:
            # logger.debug(f"Entity from topic '{topic}' was not selected for blacklisting.")

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON message from topic {msg.topic()}: {msg.value()}", exc_info=True)
    except Exception as e:
        logger.error(f"Error processing message from topic {msg.topic()}: {e}", exc_info=True)


# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting Blacklist Processor (Consumer/Producer)...")

    # Validate essential config
    if not all([KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID, KAFKA_USERS_TOPIC, KAFKA_MERCHANTS_TOPIC, KAFKA_BLACKLIST_TOPIC]):
        logger.critical("Missing required Kafka environment variables. Exiting.")
        sys.exit(1)

    # Create output topic if needed
    create_topic_if_not_exists(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BLACKLIST_TOPIC)

    # Create Kafka clients
    consumer = create_kafka_consumer(CONSUMER_GROUP_ID, [KAFKA_USERS_TOPIC, KAFKA_MERCHANTS_TOPIC])
    producer = create_kafka_producer()

    # Setup signal handling for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while running:
            msg = consumer.poll(timeout=1.0) # Poll for messages
            process_message(msg, producer, KAFKA_BLACKLIST_TOPIC)
            # Optional: Trigger producer flush periodically if needed, e.g., producer.flush(0.1)
    except KeyboardInterrupt:
         logger.info("Manual interruption detected.")
    except Exception as e:
         logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        if producer:
            logger.info("Flushing final Kafka producer messages...")
            remaining = producer.flush(timeout=30)
            if remaining > 0:
                logger.warning(f"{remaining} blacklist messages may not have been delivered.")
            else:
                logger.info("All blacklist messages flushed successfully.")
        logger.info("Blacklist Processor finished.")