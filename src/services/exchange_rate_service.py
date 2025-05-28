# src/services/exchange_rate_service.py
import requests
import json
import os
import logging
import sys
from datetime import datetime, timezone # Use timezone-aware datetime
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic
from src.utils.config import KAFKA, EXCHANGE_RATE_API, SCHEMA_REGISTRY
from src.utils.schema_registry import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = KAFKA["TOPICS"]["exchange_rates"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
SCHEMA_REGISTRY_URL = SCHEMA_REGISTRY["URL"]

APP_ID = EXCHANGE_RATE_API["APP_ID"]
BASE_CURRENCY = EXCHANGE_RATE_API["BASE_CURRENCY"]

# --- Kafka Producer Setup ---
def create_kafka_producer():
    """Creates and returns Kafka Producer instances (both Avro and regular)."""
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'exchange-rate-producer',
        'compression.type': 'gzip',
        'schema.registry.url': SCHEMA_REGISTRY_URL
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        producer_config.update({
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        })
        logger.info("Configuring Kafka Producer with SASL.")

    try:
        # Create both Avro and regular producers
        avro_producer = AvroProducer(producer_config)
        regular_producer = Producer(producer_config)
        logger.info(f"Kafka Producers connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return avro_producer, regular_producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producers: {e}", exc_info=True)
        sys.exit(1) # Exit if producers cannot be created

def delivery_report(err, msg):
    """ Callback function for Kafka Produce results. """
    if err is not None:
        logger.error(f"Message delivery failed for key {msg.key()}: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} | Key: {msg.key()}")

# --- Topic Creation ---
def create_topic_if_not_exists(bootstrap_servers, topic_name):
    """Creates the Kafka topic if it doesn't exist."""
    admin_config = {'bootstrap.servers': bootstrap_servers}
    if KAFKA_USERNAME and KAFKA_PASSWORD:
         admin_config.update({
            'security.protocol': 'SASL_SSL', # Or SASL_SSL
            'sasl.mechanism': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD
        })
    admin_client = AdminClient(admin_config)
    topic_metadata = admin_client.list_topics(timeout=10)

    if topic_name not in topic_metadata.topics:
        logger.info(f"Topic '{topic_name}' not found. Attempting to create...")
        new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
        futures = admin_client.create_topics([new_topic])
        try:
            futures[topic_name].result() # Wait for the topic creation result
            logger.info(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            # Decide if you want to exit or continue without the topic
            # sys.exit(1)
    else:
        logger.info(f"Topic '{topic_name}' already exists.")

# --- Core Logic ---
def fetch_exchange_rates(app_id: str, base_currency: str) -> dict | None:
    """Fetches latest exchange rates from Open Exchange Rates API."""
    if not app_id:
        logger.error("Open Exchange Rates API Key (APP_ID) is missing.")
        return None
    url = f"https://openexchangerates.org/api/latest.json?app_id={app_id}&base={base_currency}"
    try:
        response = requests.get(url, timeout=15) # Add timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logger.info(f"Fetched {len(data.get('rates', {}))} exchange rates with base {data.get('base', 'N/A')}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from Open Exchange Rates API: {e}", exc_info=True)
        return None
    except json.JSONDecodeError as e:
         logger.error(f"Failed to decode JSON response from API: {e}", exc_info=True)
         return None

def get_exchange_rate_schema():
    """Get the Avro schema for exchange rates"""
    schema_registry = SchemaRegistry(SCHEMA_REGISTRY_URL)
    return schema_registry.get_schema("exchange_rate")

def publish_rates_to_kafka(avro_producer: AvroProducer, regular_producer: Producer, topic: str, rates_data: dict):
    """Publishes individual exchange rates to Kafka using Avro serialization."""
    if not rates_data or 'rates' not in rates_data or 'base' not in rates_data or 'timestamp' not in rates_data:
        logger.error("Invalid rates data received, cannot publish.")
        return 0

    base_currency = rates_data['base']
    api_timestamp_unix = rates_data['timestamp']
    api_timestamp_iso = datetime.fromtimestamp(api_timestamp_unix, tz=timezone.utc).isoformat()
    
    # Get the Avro schema
    exchange_rate_schema = get_exchange_rate_schema()
    if not exchange_rate_schema:
        logger.error("Failed to get exchange rate schema. Falling back to JSON serialization.")
        use_avro = False
    else:
        use_avro = True
        logger.info("Using Avro serialization for exchange rates")

    published_count = 0
    for target_currency, rate in rates_data['rates'].items():
        message_key = f"{base_currency}_{target_currency}"
        message_payload = {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "rate": float(rate), # Ensure rate is float
            "api_timestamp_iso_utc": api_timestamp_iso,
            "fetch_timestamp_iso_utc": datetime.now(timezone.utc).isoformat() # Add fetch time
        }
        
        try:
            if use_avro:
                # Use Avro serialization
                avro_producer.produce(
                    topic=topic,
                    key=message_key,
                    value=message_payload,
                    value_schema=json.dumps(exchange_rate_schema),
                    callback=delivery_report
                )
            else:
                # Fallback to regular JSON serialization
                regular_producer.produce(
                    topic=topic,
                    key=message_key.encode('utf-8'),
                    value=json.dumps(message_payload).encode('utf-8'),
                    callback=delivery_report
                )
                
            published_count += 1
            if use_avro:
                avro_producer.poll(0)
            else:
                regular_producer.poll(0)
                
        except BufferError:
            logger.warning("Kafka producer queue is full. Flushing...")
            if use_avro:
                avro_producer.flush(timeout=5)
            else:
                regular_producer.flush(timeout=5)
                
            # Retry producing the current message
            try:
                if use_avro:
                    avro_producer.produce(
                        topic=topic,
                        key=message_key,
                        value=message_payload,
                        value_schema=json.dumps(exchange_rate_schema),
                        callback=delivery_report
                    )
                else:
                    regular_producer.produce(
                        topic=topic,
                        key=message_key.encode('utf-8'),
                        value=json.dumps(message_payload).encode('utf-8'),
                        callback=delivery_report
                    )
                published_count += 1
            except Exception as e:
                logger.error(f"Failed to produce message for key {message_key} after buffer full: {e}", exc_info=True)
                
        except Exception as e:
            logger.error(f"Failed to produce message for key {message_key}: {e}", exc_info=True)
            # Try fallback to regular JSON if Avro fails
            if use_avro:
                try:
                    logger.warning(f"Falling back to JSON serialization for key {message_key}")
                    regular_producer.produce(
                        topic=topic,
                        key=message_key.encode('utf-8'),
                        value=json.dumps(message_payload).encode('utf-8'),
                        callback=delivery_report
                    )
                    published_count += 1
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed for key {message_key}: {fallback_error}", exc_info=True)

    logger.info(f"Attempted to publish {published_count} rate messages to topic '{topic}'.")
    return published_count

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting exchange rate fetch and publish process...")

    # Validate essential config
    if not APP_ID:
        logger.critical("Missing required environment variable: OPENEXCHANGERATES_APP_ID. Exiting.")
        sys.exit(1)
    if not KAFKA_BOOTSTRAP_SERVERS:
         logger.critical("Missing required environment variable: KAFKA_BOOTSTRAP_SERVERS. Exiting.")
         sys.exit(1)

    # Ensure topic exists
    create_topic_if_not_exists(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # Fetch data
    exchange_rate_data = fetch_exchange_rates(APP_ID, BASE_CURRENCY)

    if exchange_rate_data:
        # Create producers (Avro and regular)
        avro_producer, regular_producer = create_kafka_producer()

        # Publish data
        publish_rates_to_kafka(avro_producer, regular_producer, KAFKA_TOPIC, exchange_rate_data)

        # Ensure all messages are sent before exiting
        logger.info("Flushing Kafka messages...")
        # Flush both producers
        avro_remaining = avro_producer.flush(timeout=15)
        regular_remaining = regular_producer.flush(timeout=15)
        
        total_remaining = avro_remaining + regular_remaining
        if total_remaining > 0:
            logger.warning(f"{total_remaining} messages may not have been delivered after flush.")
        else:
            logger.info("All messages flushed successfully.")
    else:
        logger.warning("No exchange rate data fetched. Nothing published.")

    logger.info("Exchange rate fetch and publish process finished.")
    