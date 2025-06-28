# src/producers/transaction_producer.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rand
from pyspark.sql.types import IntegerType, DoubleType, StringType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TransactionProducer")


TRANSACTION_SCHEMA = {
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
    },
    "required": [
        "transaction_id", "user_id", "amount", "currency",
        "merchant_id", "merchant_name", "merchant_category", "card_type",
        "timestamp", "location_country", "location_city",
        "latitude", "longitude"
    ]
}

class TransactionProducer:

    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder\
            .appName("TransactionProducer")\
            .getOrCreate()
        
    def load_kaggle_dataset(self):
        """Load all Kaggle datasets and JSON files into the data lake"""
        try:
            # Load and save transactions dataset
            transactions_df = self.spark.read.csv(
                "transactions_data.csv",
                header=True,
                inferSchema=True
            )
            transactions_df.write.format("parquet")\
                .mode("overwrite")\
                .save("data_lake/transactions")
            logger.info("Successfully loaded transactions dataset")

            # Load and save users dataset
            users_df = self.spark.read.csv(
                "users_data.csv",
                header=True,
                inferSchema=True
            )
            users_df.write.format("parquet")\
                .mode("overwrite")\
                .save("data_lake/users")
            logger.info("Successfully loaded users dataset")

            # Load and save cards dataset
            cards_df = self.spark.read.csv(
                "cards_data.csv",
                header=True,
                inferSchema=True
            )
            cards_df.write.format("parquet")\
                .mode("overwrite")\
                .save("data_lake/cards")
            logger.info("Successfully loaded cards dataset")

            # Load and save fraud labels JSON
            fraud_df = self.spark.read.json("train_fraud_labels.json")
            fraud_df.write.format("parquet")\
                .mode("overwrite")\
                .save("data_lake/fraud_labels")
            logger.info("Successfully loaded fraud labels")

            # Load and save MCC codes JSON
            mcc_df = self.spark.read.json("mcc_codes.json")
            mcc_df.write.format("parquet")\
                .mode("overwrite")\
                .save("data_lake/mcc_codes")
            logger.info("Successfully loaded MCC codes")
            
            logger.info("All datasets loaded successfully into data lake")
            
        except Exception as e:
            logger.error(f"Error loading datasets: {str(e)}")
            raise

    def run(self):
        """Main execution method"""
        logger.info("Starting Transaction Producer")
        try:
            self.load_kaggle_dataset()
            logger.info("Data processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in run loop: {str(e)}")
            raise
        finally:
            logger.info("Shutting down Transaction Producer")
            if hasattr(self, 'spark'):
                self.spark.stop()

if __name__ == "__main__":
    TransactionProducer().run()
