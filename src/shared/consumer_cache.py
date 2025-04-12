import json
import os
import logging
import random
from confluent_kafka import Consumer, KafkaException
import sys
import signal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class CacheConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.topic = topic
        self.cache = {} # Store items by key (user_id or merchant_id)
        self.keys = []  # Keep track of keys for random sampling

        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            # Increase session timeout and heartbeat interval for stability
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'enable.auto.commit': True # Or False if you need manual commit logic
        }

        try:
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe([self.topic])
            logger.info(f"CacheConsumer subscribed to topic '{self.topic}' with group '{group_id}'")
        except KafkaException as e:
            logger.error(f"Failed to create Kafka Consumer for topic {self.topic}: {e}")
            raise

    def poll_messages(self, num_messages, timeout):
        """Polls for messages and updates the cache."""
        messages_processed = 0
        try:
            msgs = self.consumer.consume(num_messages=num_messages, timeout=timeout)
            if not msgs:
                logger.debug(f"No messages received from {self.topic} in the last poll.")
                return 0

            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka Consumer error on topic {self.topic}: {msg.error()}")
                    continue

                try:
                    value = msg.value()
                    if value:
                        data = json.loads(value.decode('utf-8'))
                        # Determine key based on expected fields
                        key = data.get("merchant_id") or data.get("user_id") or (msg.key().decode('utf-8') if msg.key() else None)
                        if key:
                            if key not in self.cache:
                                self.keys.append(key) # Add new key for random sampling
                            self.cache[key] = data
                            messages_processed += 1
                        else:
                             logger.warning(f"Message missing key (user_id/merchant_id) on topic {self.topic}: {data}")
                    else:
                        # Handle tombstone messages if necessary (key exists, value is None)
                        key = msg.key().decode('utf-8') if msg.key() else None
                        if key and key in self.cache:
                            logger.info(f"Removing key {key} from cache due to tombstone message.")
                            del self.cache[key]
                            if key in self.keys: self.keys.remove(key)


                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON message from topic {self.topic}: {msg.value()}", exc_info=True)
                except Exception as e:
                    logger.error(f"Error processing message from topic {self.topic}: {e}", exc_info=True)

        except Exception as e:
             logger.error(f"Error during Kafka consume loop for topic {self.topic}: {e}", exc_info=True)

        if messages_processed > 0:
             logger.info(f"Processed {messages_processed} messages into cache for topic {self.topic}. Cache size: {len(self.cache)}")
        return messages_processed


    def close(self):
        """Closes the Kafka consumer."""
        if hasattr(self, 'consumer') and self.consumer:
            self.consumer.close()
            logger.info(f"Closed Kafka consumer for topic {self.topic}.")


def main():

    def handle_shutdown(signum, frame):
        logger.info("Shutting down gracefully...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Read environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    group_id = os.environ.get("KAFKA_CONSUMER_GROUP_ID")
    topic = os.environ.get("KAFKA_TOPIC")

    # Validate env vars
    if not all([bootstrap_servers, group_id, topic]):
        logger.error("Missing one or more required environment variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID, KAFKA_TOPIC")
        sys.exit(1)

    consumer = CacheConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic
    )

    logger.info("Starting cache polling loop...")

    try:
        while True:
            consumer.poll_messages(num_messages=100, timeout=1.0)
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        consumer.close()

if __name__ == "__main__":
    main()
