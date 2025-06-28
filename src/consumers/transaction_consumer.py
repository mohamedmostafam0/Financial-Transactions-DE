#!/usr/bin/env python3
# src/consumers/transaction_consumer.py

import json
import logging
import signal
import sys
import time
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from google.cloud import storage

from src.utils.config import KAFKA, BIGQUERY, SCHEMA_REGISTRY
from src.utils.schema_registry import SchemaRegistry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TransactionConsumer")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
KAFKA_TOPIC = KAFKA["TOPICS"]["transactions"]
CONSUMER_GROUP_ID = "transaction_consumer"
SCHEMA_REGISTRY_URL = SCHEMA_REGISTRY["URL"]

# GCS configuration
GCP_PROJECT_ID = BIGQUERY["PROJECT_ID"]
GCS_BUCKET = BIGQUERY["PARQUET_OUTPUT_BUCKET"]
GCS_PREFIX = f"{BIGQUERY['PARQUET_OUTPUT_PREFIX']}/transactions"

# Batch settings
BATCH_SIZE = 100  # Number of messages to batch before writing to GCS (higher for transactions)
FLUSH_INTERVAL = 60  # Seconds between forced flushes

# Parquet settings
ROW_GROUP_SIZE = 100000  # Row group size for Parquet files
COMPRESSION = 'snappy'  # Compression codec for Parquet files

# Global flag for graceful shutdown
running = True

class TransactionConsumer:
    """Consumer for transaction data from Kafka to GCS data lake"""
    
    def __init__(self):
        """Initialize the transaction consumer"""
        if not KAFKA_BOOTSTRAP_SERVERS:
            logger.critical("KAFKA_BOOTSTRAP_SERVERS not set.")
            sys.exit(1)
            
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'schema.registry.url': SCHEMA_REGISTRY_URL
        }
        
        if KAFKA_USERNAME and KAFKA_PASSWORD:
            sasl_config = {
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': KAFKA_USERNAME,
                'sasl.password': KAFKA_PASSWORD,
            }
            consumer_config.update(sasl_config)
            
        # Create both Avro and regular consumers for flexibility
        try:
            self.avro_consumer = AvroConsumer(consumer_config)
            logger.info("Created Avro consumer for transactions")
            self.use_avro = True
        except Exception as e:
            logger.error(f"Failed to create Avro consumer: {e}")
            logger.warning("Falling back to regular consumer")
            self.avro_consumer = None
            self.use_avro = False
            
        # Create regular consumer as fallback
        self.regular_consumer = Consumer(consumer_config)
        
        # Initialize GCS client
        self.storage_client = storage.Client(project=GCP_PROJECT_ID)
        
        # Initialize message buffer for batching
        self.message_buffer = []
        self.last_flush_time = time.time()
        self.transaction_count = 0
        
        # Ensure GCS bucket exists
        self._ensure_bucket_exists()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        global running
        logger.info("Shutdown signal received.")
        running = False
        
    def _ensure_bucket_exists(self):
        """Ensure the GCS bucket exists"""
        try:
            bucket = self.storage_client.bucket(GCS_BUCKET)
            if not bucket.exists():
                logger.warning(f"Bucket {GCS_BUCKET} does not exist. Creating it...")
                bucket = self.storage_client.create_bucket(GCS_BUCKET)
                logger.info(f"Created bucket {GCS_BUCKET}")
            else:
                logger.info(f"Bucket {GCS_BUCKET} already exists")
        except Exception as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    def process_message(self, message):
        """Process a single transaction message from Kafka and add to buffer"""
        try:
            # Parse message value - Avro messages are already deserialized to dict
            # Regular messages need JSON parsing
            message_value = message.value()
            
            if self.use_avro:
                # Avro consumer automatically deserializes to dict
                data = message_value
            elif isinstance(message_value, bytes):
                # Regular consumer returns bytes that need JSON parsing
                data = json.loads(message_value.decode('utf-8'))
            else:
                data = message_value
                
            # Add received timestamp if not present
            if 'received_at' not in data:
                data['received_at'] = datetime.now(timezone.utc).isoformat()
                
            # Add to buffer
            self.message_buffer.append(data)
            self.transaction_count += 1
            
            # Log progress periodically
            if self.transaction_count % 1000 == 0:
                logger.info(f"Processed {self.transaction_count} transactions so far")
                
            # Check if we should flush the buffer
            should_flush = len(self.message_buffer) >= BATCH_SIZE
            time_to_flush = (time.time() - self.last_flush_time) >= FLUSH_INTERVAL
            
            if should_flush or time_to_flush:
                self._flush_buffer_to_gcs()
                
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def _flush_buffer_to_gcs(self):
        """Flush the message buffer to GCS as Parquet"""
        if not self.message_buffer:
            logger.debug("Buffer is empty, nothing to flush")
            self.last_flush_time = time.time()
            return
            
        try:
            # Create a unique filename based on timestamp and UUID
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            year = datetime.now().strftime("%Y")
            month = datetime.now().strftime("%m")
            day = datetime.now().strftime("%d")
            hour = datetime.now().strftime("%H")
            
            # Create path with partitioning by year/month/day/hour
            file_path = f"{GCS_PREFIX}/{year}/{month}/{day}/{hour}/transactions_{timestamp}_{unique_id}.parquet"
            
            # Convert buffer to DataFrame
            df = pd.DataFrame(self.message_buffer)
            
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Create an in-memory buffer
            buf = BytesIO()
            
            # Write to Parquet format
            pq.write_table(
                table, 
                buf,
                compression=COMPRESSION,
                row_group_size=ROW_GROUP_SIZE
            )
            
            # Reset buffer position to beginning
            buf.seek(0)
            
            # Upload to GCS
            bucket = self.storage_client.bucket(GCS_BUCKET)
            blob = bucket.blob(file_path)
            blob.upload_from_file(buf, content_type='application/octet-stream')
            
            logger.info(f"Flushed {len(self.message_buffer)} transactions to GCS as Parquet: gs://{GCS_BUCKET}/{file_path}")
            
            # Clear buffer and update last flush time
            buffer_size = len(self.message_buffer)
            self.message_buffer = []
            self.last_flush_time = time.time()
            
            return buffer_size
        except Exception as e:
            logger.error(f"Error flushing buffer to GCS: {e}")
            # Don't clear buffer on error so we can retry
            return 0
    
    def run(self):
        """Main function to consume transactions and load to GCS"""
        global running
        
        logger.info(f"Transaction consumer started for topic: {KAFKA_TOPIC}")
        logger.info(f"Consumer group ID: {CONSUMER_GROUP_ID}")
        logger.info(f"GCS destination: gs://{GCS_BUCKET}/{GCS_PREFIX}/ (Parquet format)")
        
        # Subscribe to topic with the appropriate consumer
        if self.use_avro:
            self.avro_consumer.subscribe([KAFKA_TOPIC])
        else:
            self.regular_consumer.subscribe([KAFKA_TOPIC])
            
        logger.info(f"Using {'Avro' if self.use_avro else 'regular JSON'} deserialization")
        
        try:
            while running:
                # Poll for messages from the appropriate consumer
                if self.use_avro:
                    msg = self.avro_consumer.poll(timeout=1.0)
                else:
                    msg = self.regular_consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error: {msg.error()}")
                else:
                    try:
                        # Process message
                        success = self.process_message(msg)
                        
                        # Commit offset if processing was successful
                        if success:
                            if self.use_avro:
                                self.avro_consumer.commit(msg)
                            else:
                                self.regular_consumer.commit(msg)
                    except SerializerError as e:
                        logger.error(f"Serialization error: {e}")
                        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Flush any remaining messages in the buffer
            if self.message_buffer:
                logger.info(f"Flushing {len(self.message_buffer)} remaining transactions as Parquet before shutdown")
                self._flush_buffer_to_gcs()
                
            # Close consumers
            if self.use_avro:
                self.avro_consumer.close()
            else:
                self.regular_consumer.close()
            logger.info("Transaction consumer stopped")

def main():
    """Entry point for the transaction consumer"""
    consumer = TransactionConsumer()
    consumer.run()

if __name__ == "__main__":
    main()
