#!/usr/bin/env python3
# src/consumers/economic_indicators_consumer.py

import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Dict, Any, List

from confluent_kafka import Consumer, KafkaException
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

from src.utils.config import KAFKA, BIGQUERY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EconomicIndicatorsConsumer")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
KAFKA_TOPIC = KAFKA["TOPICS"]["economic_indicators"]
CONSUMER_GROUP_ID = "economic_indicators_consumer"

# BigQuery configuration
BQ_PROJECT_ID = BIGQUERY["PROJECT_ID"]
BQ_DATASET = BIGQUERY["DATASET"]
BQ_TABLE = "economic_indicators"
GCS_BUCKET = BIGQUERY["PARQUET_OUTPUT_BUCKET"]
GCS_PREFIX = f"{BIGQUERY['PARQUET_OUTPUT_PREFIX']}/economic_indicators"

# Global flag for graceful shutdown
running = True

class EconomicIndicatorsConsumer:
    """Consumer for economic indicators data from Kafka to BigQuery"""
    
    def __init__(self):
        """Initialize the economic indicators consumer"""
        if not KAFKA_BOOTSTRAP_SERVERS:
            logger.critical("KAFKA_BOOTSTRAP_SERVERS not set.")
            sys.exit(1)
            
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        if KAFKA_USERNAME and KAFKA_PASSWORD:
            sasl_config = {
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': KAFKA_USERNAME,
                'sasl.password': KAFKA_PASSWORD,
            }
            consumer_config.update(sasl_config)
            
        # Create consumer
        self.consumer = Consumer(consumer_config)
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=BQ_PROJECT_ID)
        
        # Initialize GCS client
        self.storage_client = storage.Client(project=BQ_PROJECT_ID)
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Ensure BigQuery table exists
        self._ensure_table_exists()
        
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        global running
        logger.info("Shutdown signal received.")
        running = False
        
    def _ensure_table_exists(self):
        """Ensure the BigQuery table exists"""
        try:
            dataset_ref = self.bq_client.dataset(BQ_DATASET)
            table_ref = dataset_ref.table(BQ_TABLE)
            
            try:
                self.bq_client.get_table(table_ref)
                logger.info(f"Table {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE} already exists")
            except NotFound:
                # Table doesn't exist, create it
                schema = [
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("indicator_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("indicator_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("value", "FLOAT", mode="REQUIRED"),
                    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("frequency", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("source", "STRING", mode="REQUIRED")
                ]
                
                table = bigquery.Table(table_ref, schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp"
                )
                
                self.bq_client.create_table(table)
                logger.info(f"Created table {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}")
                
        except Exception as e:
            logger.error(f"Error ensuring table exists: {e}")
            raise
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            # Parse message value
            message_value = message.value()
            if isinstance(message_value, bytes):
                data = json.loads(message_value.decode('utf-8'))
            else:
                data = message_value
                
            # Prepare row for BigQuery
            row = {
                "timestamp": data.get("timestamp"),
                "indicator_id": data.get("indicator_id"),
                "indicator_name": data.get("indicator_name"),
                "value": data.get("value"),
                "date": data.get("date"),
                "country": data.get("country"),
                "frequency": data.get("frequency"),
                "source": data.get("source")
            }
            
            # Insert row into BigQuery
            errors = self.bq_client.insert_rows_json(
                f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
                [row]
            )
            
            if errors:
                logger.error(f"Error inserting row into BigQuery: {errors}")
            else:
                logger.info(f"Successfully inserted {data.get('indicator_name')} into BigQuery")
                
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def run(self):
        """Main function to consume economic indicators and load to BigQuery"""
        global running
        
        logger.info(f"Economic indicators consumer started for topic: {KAFKA_TOPIC}")
        logger.info(f"Consumer group ID: {CONSUMER_GROUP_ID}")
        logger.info(f"BigQuery destination: {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}")
        
        # Subscribe to topic
        self.consumer.subscribe([KAFKA_TOPIC])
        
        try:
            while running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error: {msg.error()}")
                else:
                    # Process message
                    success = self.process_message(msg)
                    
                    # Commit offset if processing was successful
                    if success:
                        self.consumer.commit(msg)
                        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Close consumer
            self.consumer.close()
            logger.info("Economic indicators consumer stopped")

def main():
    """Entry point for the economic indicators consumer"""
    consumer = EconomicIndicatorsConsumer()
    consumer.run()

if __name__ == "__main__":
    main()
