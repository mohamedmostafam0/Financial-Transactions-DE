# src/consumers/fraud_blacklist.py
# Fraud detection and blacklisting service

import uuid
import json
import logging
import random
import time
import os
from datetime import datetime, timezone
import sys
import signal
from faker import Faker
from confluent_kafka import Producer
from src.utils.config import BLACKLIST_PROBABILITY, POSTGRES, KAFKA, BIGQUERY
from src.utils.postgres_db import PostgresDB

# Extract PostgreSQL configuration
PG_ENABLED = POSTGRES["ENABLED"]

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()
running = True # Global flag for graceful shutdown

# PostgreSQL configuration from config.py
PG_ENABLED = POSTGRES["ENABLED"]

# --- Signal Handling ---
def shutdown_handler(signum, frame):
    global running
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    running = False

# --- Kafka Producer Setup ---
def create_kafka_producer():
    """Create a Kafka producer instance."""
    try:
        # Configure Kafka producer
        producer_config = {
            'bootstrap.servers': KAFKA["BOOTSTRAP_SERVERS"],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA["USERNAME"],
            'sasl.password': KAFKA["PASSWORD"],
            'client.id': 'fraud-blacklist-producer'
        }
        
        # Create producer instance
        producer = Producer(producer_config)
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

def publish_to_kafka(producer, topic, data):
    """Publish data to a Kafka topic.
    
    Args:
        producer: Kafka producer instance
        topic (str): Kafka topic name
        data (dict): Data to publish
        
    Returns:
        bool: True if publish successful, False otherwise
    """
    if not producer:
        logger.error("Kafka producer not available")
        return False
        
    try:
        # Convert data to JSON string
        message_value = json.dumps(data).encode('utf-8')
        
        # Publish to Kafka topic
        producer.produce(
            topic=topic,
            value=message_value,
            key=str(data.get('entity_id', uuid.uuid4())).encode('utf-8'),
            callback=lambda err, msg: delivery_callback(err, msg, data)
        )
        producer.flush(timeout=10)
        return True
    except Exception as e:
        logger.error(f"❌ Failed to publish to Kafka topic {topic}: {e}")
        return False

def delivery_callback(err, msg, data):
    """Callback function for Kafka producer delivery reports."""
    if err is not None:
        logger.error(f"❌ Message delivery failed: {err}")
    else:
        logger.info(f"✅ Published blacklist entry to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# --- Blacklist Operations ---
def add_to_blacklist(entity_type, entity_id, reason=None, db=None, producer=None):
    """Adds an entity to the blacklist and publishes to Kafka topic.
    
    Args:
        entity_type (str): Type of entity ('user' or 'merchant')
        entity_id (str): ID of the entity
        reason (str, optional): Reason for blacklisting
        db (PostgresDB, optional): Database connection
        producer (Producer, optional): Kafka producer
    
    Returns:
        bool: True if successfully added to blacklist, False otherwise
    """
    if not db or not db.is_connected():
        logger.error("PostgreSQL database not connected. Cannot add to blacklist.")
        return False

    # Generate timestamp for the blacklist entry
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Create blacklist entry
    if reason is None:
        reason = f"Suspicious activity detected at {timestamp}"
        
    # Create blacklist entry following the Avro schema
    blacklist_entry = {
        "entity_type": entity_type,
        "entity_id": str(entity_id),
        "reason": reason,
        "timestamp": timestamp
    }
    
    # Store in PostgreSQL
    db_success = db.insert_blacklist(blacklist_entry)
    if not db_success:
        logger.error(f"❌ Failed to add {entity_type} ID {entity_id} to blacklist in database")
        return False
        
    # Publish to Kafka topic
    blacklist_topic = KAFKA["TOPICS"].get("blacklist")
    if producer and blacklist_topic:
        kafka_success = publish_to_kafka(producer, blacklist_topic, blacklist_entry)
        if kafka_success:
            logger.info(f"✅ Added {entity_type} ID {entity_id} to blacklist and published to Kafka topic")
            return True
        else:
            logger.warning(f"⚠️ Added {entity_type} ID {entity_id} to blacklist in database but failed to publish to Kafka")
            return True  # Still return True since it was added to the database
    else:
        logger.warning(f"⚠️ Kafka producer or topic not configured. {entity_type} ID {entity_id} added to blacklist in database only")
        return True  # Still return True since it was added to the database

def is_blacklisted(entity_type, entity_id, db=None):
    """Checks if an entity is blacklisted.
    
    Args:
        entity_type (str): Type of entity ('user' or 'merchant')
        entity_id (str): ID of the entity
        db (PostgresDB, optional): Database connection
    
    Returns:
        bool: True if entity is blacklisted, False otherwise
    """
    if not db or not db.is_connected():
        logger.error("PostgreSQL database not connected. Cannot check blacklist.")
        return False
    
    return db.is_blacklisted(entity_type, entity_id)

def randomly_blacklist_entity(entity_type, entity_id, db=None):
    """Randomly decides whether to blacklist an entity based on probability.
    
    Args:
        entity_type (str): Type of entity ('user' or 'merchant')
        entity_id (str): ID of the entity
        db (PostgresDB, optional): Database connection
    
    Returns:
        bool: True if entity was blacklisted, False otherwise
    """
    if not db or not db.is_connected():
        return False
        
    # Check if already blacklisted
    if is_blacklisted(entity_type, entity_id, db):
        return True
        
    # Randomly decide whether to blacklist based on probability
    if random.random() < BLACKLIST_PROBABILITY:
        reason = fake.sentence()
        return add_to_blacklist(entity_type, entity_id, reason, db)
    
    return False

def process_users_for_blacklisting(db, producer):
    """Check users in the database and randomly flag some as fraudulent."""
    try:
        # Get a batch of users to check
        users = db.get_users_batch(limit=100)
        if not users:
            logger.info("No users found to check for blacklisting")
            return 0
            
        blacklisted_count = 0
        for user in users:
            # Skip already blacklisted users
            if db.is_blacklisted('user', user['user_id']):
                continue
                
            # Randomly decide whether to blacklist based on probability
            if random.random() < BLACKLIST_PROBABILITY:
                reason = f"Suspicious user activity detected for user {user['user_id']}. Flagged by automatic fraud detection."
                if add_to_blacklist('user', user['user_id'], reason, db, producer):
                    blacklisted_count += 1
                    
        if blacklisted_count > 0:
            logger.info(f"Blacklisted {blacklisted_count} users out of {len(users)} checked")
        return blacklisted_count
    except Exception as e:
        logger.error(f"Error processing users for blacklisting: {e}")
        return 0

def process_merchants_for_blacklisting(db, producer):
    """Check merchants in the database and randomly flag some as fraudulent."""
    try:
        # Get a batch of merchants to check
        merchants = db.get_merchants_batch(limit=100)
        if not merchants:
            logger.info("No merchants found to check for blacklisting")
            return 0
            
        blacklisted_count = 0
        for merchant in merchants:
            # Skip already blacklisted merchants
            if db.is_blacklisted('merchant', merchant['merchant_id']):
                continue
                
            # Randomly decide whether to blacklist based on probability
            if random.random() < BLACKLIST_PROBABILITY:
                reason = f"Suspicious merchant activity detected for {merchant['merchant_name']}. Flagged by automatic fraud detection."
                if add_to_blacklist('merchant', merchant['merchant_id'], reason, db, producer):
                    blacklisted_count += 1
                    
        if blacklisted_count > 0:
            logger.info(f"Blacklisted {blacklisted_count} merchants out of {len(merchants)} checked")
        return blacklisted_count
    except Exception as e:
        logger.error(f"Error processing merchants for blacklisting: {e}")
        return 0

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting Fraud Detection and Blacklisting Service...")

    # Initialize PostgreSQL database
    db = None
    if PG_ENABLED:
        db = PostgresDB()
        if db.is_connected():
            logger.info("PostgreSQL database initialized and connected")
        else:
            logger.error("PostgreSQL database connection failed")
            sys.exit(1)
    else:
        logger.error("PostgreSQL is disabled. Cannot run blacklist service.")
        sys.exit(1)

    # Setup signal handling for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Initialize Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.warning("Kafka producer initialization failed. Blacklist entries will only be stored in the database.")
    else:
        blacklist_topic = KAFKA["TOPICS"].get("blacklist")
        if blacklist_topic:
            logger.info(f"Kafka producer initialized. Publishing to topic: {blacklist_topic}")
        else:
            logger.warning("Blacklist topic not configured in KAFKA.TOPICS. Check your configuration.")

    # Define processing interval (in seconds)
    PROCESSING_INTERVAL = 60  # Process every minute
    
    logger.info(f"Fraud detection service started. Processing interval: {PROCESSING_INTERVAL} seconds")
    
    # Main processing loop
    try:
        while running:
            start_time = time.time()
            
            # Process users for blacklisting
            users_blacklisted = process_users_for_blacklisting(db, producer)
            
            # Process merchants for blacklisting
            merchants_blacklisted = process_merchants_for_blacklisting(db, producer)
            
            total_blacklisted = users_blacklisted + merchants_blacklisted
            if total_blacklisted > 0:
                logger.info(f"Total entities blacklisted in this cycle: {total_blacklisted}")
            
            # Calculate remaining time to sleep
            elapsed = time.time() - start_time
            sleep_time = max(0, PROCESSING_INTERVAL - elapsed)
            
            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.2f} seconds until next processing cycle")
                # Sleep in small increments to respond to shutdown signals quickly
                for _ in range(int(sleep_time)):
                    if not running:
                        break
                    time.sleep(1)
                    
    except KeyboardInterrupt:
        logger.info("Manual interruption detected.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        if db:
            db.close()
            logger.info("Database connection closed.")
        logger.info("Fraud detection service finished.")