#!/usr/bin/env python3
# src/services/economic_indicators_service.py

import requests
import json
import time
import logging
import signal
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from src.utils.config import KAFKA, SCHEMA_REGISTRY, ECONOMIC_API

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EconomicIndicatorsService")

# FRED API configuration
FRED_API_KEY = ECONOMIC_API["FRED_API_KEY"]
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
UPDATE_INTERVAL = ECONOMIC_API["UPDATE_INTERVAL"]

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = KAFKA["BOOTSTRAP_SERVERS"]
KAFKA_USERNAME = KAFKA["USERNAME"]
KAFKA_PASSWORD = KAFKA["PASSWORD"]
KAFKA_TOPIC = KAFKA["TOPICS"]["economic_indicators"]

# Economic indicators to track
INDICATORS = {
    "UNRATE": {
        "name": "Unemployment Rate",
        "frequency": "monthly",
        "country": "US"
    },
    "CPIAUCSL": {
        "name": "Consumer Price Index",
        "frequency": "monthly",
        "country": "US"
    },
    "GDP": {
        "name": "Gross Domestic Product",
        "frequency": "quarterly",
        "country": "US"
    },
    "FEDFUNDS": {
        "name": "Federal Funds Rate",
        "frequency": "monthly",
        "country": "US"
    },
    "UMCSENT": {
        "name": "Consumer Sentiment",
        "frequency": "monthly",
        "country": "US"
    },
    "RSAFS": {
        "name": "Retail Sales",
        "frequency": "monthly",
        "country": "US"
    },
    "HOUST": {
        "name": "Housing Starts",
        "frequency": "monthly",
        "country": "US"
    },
    "INDPRO": {
        "name": "Industrial Production Index",
        "frequency": "monthly",
        "country": "US"
    }
}

# Global flag for graceful shutdown
running = True

class EconomicIndicatorsService:
    """Service to fetch economic indicators from FRED API and publish to Kafka"""
    
    def __init__(self):
        """Initialize the economic indicators service"""
        if not KAFKA_BOOTSTRAP_SERVERS:
            logger.critical("KAFKA_BOOTSTRAP_SERVERS not set.")
            sys.exit(1)
            
        if not FRED_API_KEY:
            logger.critical("FRED_API_KEY not set.")
            sys.exit(1)
            
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
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        global running
        logger.info("Shutdown signal received.")
        running = False
        
    def fetch_economic_indicators(self):
        """Fetch economic indicators from FRED API"""
        indicators_data = []
        current_time = datetime.now(timezone.utc).isoformat()
        
        for series_id, metadata in INDICATORS.items():
            try:
                params = {
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 1  # Just get the latest value
                }
                
                response = requests.get(FRED_BASE_URL, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'observations' in data and len(data['observations']) > 0:
                    latest_observation = data['observations'][0]
                    
                    # Create indicator data object
                    indicator = {
                        "timestamp": current_time,
                        "indicator_id": series_id,
                        "indicator_name": metadata["name"],
                        "value": float(latest_observation["value"]) if latest_observation["value"] != "." else 0.0,
                        "date": latest_observation["date"],
                        "country": metadata["country"],
                        "frequency": metadata["frequency"],
                        "source": "FRED"
                    }
                    
                    indicators_data.append(indicator)
                    logger.info(f"Fetched {metadata['name']}: {latest_observation['value']} for {latest_observation['date']}")
                else:
                    logger.warning(f"No observations found for {series_id}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request error for {series_id}: {e}")
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"Data processing error for {series_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching {series_id}: {e}")
                
            # Add a small delay between requests to avoid rate limiting
            time.sleep(0.5)
            
        return indicators_data
    
    def publish_to_kafka(self, indicators):
        """Publish economic indicators to Kafka"""
        if not indicators:
            logger.warning("No indicators to publish")
            return
            
        for indicator in indicators:
            try:
                # Use Avro serialization
                self.avro_producer.produce(
                    topic=KAFKA_TOPIC,
                    key=indicator["indicator_id"],
                    value=indicator,
                    value_schema=json.dumps(self._get_avro_schema())
                )
                logger.info(f"Published {indicator['indicator_name']} to Kafka topic: {KAFKA_TOPIC}")
            except Exception as e:
                logger.error(f"Failed to publish with Avro: {e}")
                # Fallback to regular producer
                try:
                    self.producer.produce(
                        KAFKA_TOPIC,
                        key=indicator["indicator_id"],
                        value=json.dumps(indicator).encode('utf-8')
                    )
                    logger.info(f"Published {indicator['indicator_name']} with JSON fallback")
                except Exception as e:
                    logger.error(f"Fallback also failed: {e}")
                    
        # Flush the producer to ensure all messages are sent
        self.avro_producer.flush()
        self.producer.flush()
    
    def _get_avro_schema(self):
        """Get the Avro schema for economic indicators"""
        return {
            "type": "record",
            "name": "EconomicIndicator",
            "namespace": "com.financial.transactions",
            "fields": [
                {"name": "timestamp", "type": "string"},
                {"name": "indicator_id", "type": "string"},
                {"name": "indicator_name", "type": "string"},
                {"name": "value", "type": "double"},
                {"name": "date", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "frequency", "type": "string"},
                {"name": "source", "type": "string"}
            ]
        }
    
    def run(self):
        """Main function to fetch and publish economic indicators"""
        global running
        
        logger.info(f"Economic indicators service started for topic: {KAFKA_TOPIC}")
        logger.info(f"Using FRED API to fetch economic indicators")
        logger.info(f"Update interval: {UPDATE_INTERVAL} seconds")
        
        while running:
            try:
                # Fetch economic indicators
                indicators = self.fetch_economic_indicators()
                
                # Publish to Kafka
                self.publish_to_kafka(indicators)
                
                # Sleep until next update
                logger.info(f"Sleeping for {UPDATE_INTERVAL} seconds until next update")
                
                # Check running flag periodically during sleep
                for _ in range(UPDATE_INTERVAL // 10):
                    if not running:
                        break
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Sleep for a minute before retrying
                
        logger.info("Economic indicators service stopped")
        self.avro_producer.flush()
        self.producer.flush()
        
def main():
    """Entry point for the economic indicators service"""
    service = EconomicIndicatorsService()
    service.run()

if __name__ == "__main__":
    main()
