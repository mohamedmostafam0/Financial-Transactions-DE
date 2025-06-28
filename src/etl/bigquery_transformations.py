# src/etl/bigquery_transformations.py
import os
import logging
from google.cloud import bigquery
from src.utils.config import BIGQUERY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config variables
GCP_PROJECT_ID = BIGQUERY["PROJECT_ID"]
BQ_DATASET = BIGQUERY["DATASET"]
BQ_RAW_TABLE = f"{BIGQUERY.get('TABLE', 'raw_data')}_raw"
BQ_TRANSFORMED_TABLE = BIGQUERY.get('TABLE', 'transactions')


class BigQueryTransformations:
    def __init__(self):
        self.client = bigquery.Client(project=GCP_PROJECT_ID)
        self.dataset_ref = self.client.dataset(BQ_DATASET)

    def create_marts(self):
        """Create data marts for analytics and ML"""
        try:
            # 1. Create transaction fact table
            self.create_transaction_fact()
            
            # 2. Create user dimension
            self.create_user_dimension()
            
            # 3. Create card dimension
            self.create_card_dimension()
            
            # 4. Create merchant dimension
            self.create_merchant_dimension()
            
            # 5. Create economic indicators fact table
            self.create_economic_indicators()
            
            # 6. Create fraud analysis view
            self.create_fraud_analysis_view()
            
            logger.info("✅ All data marts created successfully")
            
        except Exception as e:
            logger.error(f"Error creating data marts: {str(e)}")
            raise

    def create_transaction_fact(self):
        """Create transaction fact table with enriched data"""
        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_transactions` AS
        SELECT
            t.id as transaction_id,
            t.client_id,
            t.card_id,
            t.merchant_id,
            t.amount,
            t.use_chip,
            t.date as transaction_date,
            c.card_brand,
            c.card_type,
            c.credit_limit,
            u.current_age,
            u.gender,
            u.latitude as user_latitude,
            u.longitude as user_longitude,
            m.merchant_city,
            m.merchant_state,
            m.zip as merchant_zip,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), t.date, MONTH) as months_since_transaction,
            CASE 
                WHEN t.amount > c.credit_limit * 0.9 THEN 'High'
                WHEN t.amount > c.credit_limit * 0.7 THEN 'Medium'
                ELSE 'Low'
            END as risk_level,
            CASE 
                WHEN t.use_chip = TRUE AND c.has_chip = FALSE THEN 'Anomaly'
                ELSE 'Normal'
            END as card_usage_anomaly
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions` t
        LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_cards` c ON t.card_id = c.id
        LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_users` u ON t.client_id = u.id
        WHERE t.date IS NOT NULL
        ORDER BY t.date DESC
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created transaction fact table")

    def create_user_dimension(self):
        """Create user dimension with demographic and location data"""
        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_users` AS
        SELECT DISTINCT
            id as user_id,
            current_age,
            birth_year,
            birth_month,
            gender,
            address,
            latitude,
            longitude,
            CASE 
                WHEN current_age < 30 THEN 'Young'
                WHEN current_age BETWEEN 30 AND 50 THEN 'Middle Age'
                ELSE 'Senior'
            END as age_group,
            TIMESTAMP(birth_year || '-' || birth_month || '-01') as birth_date
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_users`
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created user dimension")

    def create_card_dimension(self):
        """Create card dimension with card details"""
        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_cards` AS
        SELECT DISTINCT
            id as card_id,
            client_id,
            card_brand,
            card_type,
            card_number,
            expires,
            cvv,
            has_chip,
            num_cards_issued,
            credit_limit,
            CASE 
                WHEN credit_limit > 5000 THEN 'High'
                WHEN credit_limit > 2000 THEN 'Medium'
                ELSE 'Low'
            END as credit_limit_category
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_cards`
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created card dimension")

    def create_merchant_dimension(self):
        """Create merchant dimension"""
        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_merchants` AS
        SELECT DISTINCT
            merchant_id,
            merchant_city,
            merchant_state,
            zip,
            CONCAT(merchant_city, ', ', merchant_state) as merchant_location
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions`
        WHERE merchant_city IS NOT NULL
        AND merchant_state IS NOT NULL
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created merchant dimension")

    def create_economic_indicators(self):
        """Create economic indicators fact table"""
        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_economic_indicators` AS
        SELECT
            indicator,
            date,
            value,
            frequency,
            country,
            CASE indicator
                WHEN 'UNRATE' THEN 'Unemployment Rate'
                WHEN 'CPIAUCSL' THEN 'Consumer Price Index'
                WHEN 'GDP' THEN 'Gross Domestic Product'
                WHEN 'FEDFUNDS' THEN 'Federal Funds Rate'
                WHEN 'UMCSENT' THEN 'Consumer Sentiment'
                WHEN 'RSAFS' THEN 'Retail Sales'
                WHEN 'HOUST' THEN 'Housing Starts'
                WHEN 'INDPRO' THEN 'Industrial Production Index'
            END as indicator_name,
            LAG(value) OVER (PARTITION BY indicator ORDER BY date) as previous_value,
            (value - LAG(value) OVER (PARTITION BY indicator ORDER BY date)) / 
            LAG(value) OVER (PARTITION BY indicator ORDER BY date) as change_rate
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_economic_indicators`
        ORDER BY indicator, date
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created economic indicators fact table")

    def create_fraud_analysis_view(self):
        """Create view for fraud analysis with ML features"""
        query = f"""
        CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.fraud_analysis_view` AS
        SELECT
            t.transaction_id,
            t.client_id,
            t.card_id,
            t.amount,
            t.transaction_date,
            t.card_brand,
            t.card_type,
            t.use_chip,
            t.has_chip,
            t.credit_limit,
            t.current_age,
            t.gender,
            t.user_latitude,
            t.user_longitude,
            t.merchant_city,
            t.merchant_state,
            t.merchant_zip,
            t.risk_level,
            t.card_usage_anomaly,
            
            -- Transaction patterns
            COUNT(*) OVER (PARTITION BY t.client_id 
                          ORDER BY t.transaction_date 
                          RANGE BETWEEN INTERVAL 1 DAY PRECEDING 
                          AND CURRENT ROW) as daily_transaction_count,
            
            -- Location features
            ST_DISTANCE(ST_GEOGPOINT(t.user_latitude, t.user_longitude),
                        ST_GEOGPOINT(t.merchant_latitude, t.merchant_longitude)) as distance_to_merchant,
            
            -- Time-based features
            EXTRACT(HOUR FROM t.transaction_date) as transaction_hour,
            EXTRACT(DAYOFWEEK FROM t.transaction_date) as transaction_day_of_week,
            
            -- Economic indicators
            ei.value as unemployment_rate,
            ei.change_rate as unemployment_change_rate,
            
            -- Labels
            fl.is_fraud
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_transactions` t
        LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_economic_indicators` ei
            ON DATE(t.transaction_date) = ei.date
            AND ei.indicator = 'UNRATE'
        LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_fraud_labels` fl
            ON t.transaction_id = fl.transaction_id
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created fraud analysis view")

    def create_ml_features_view(self):
        """Create view with features for machine learning"""
        query = f"""
        CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.ml_features` AS
        SELECT
            fa.transaction_id,
            fa.client_id,
            fa.card_id,
            
            -- Transaction features
            fa.amount,
            fa.use_chip,
            fa.has_chip,
            fa.credit_limit,
            fa.risk_level,
            fa.card_usage_anomaly,
            
            -- User features
            fa.current_age,
            fa.gender,
            
            -- Location features
            fa.distance_to_merchant,
            fa.user_latitude,
            fa.user_longitude,
            
            -- Time features
            fa.transaction_hour,
            fa.transaction_day_of_week,
            
            -- Economic features
            fa.unemployment_rate,
            fa.unemployment_change_rate,
            
            -- Labels
            fa.is_fraud
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fraud_analysis_view` fa
        """
        
        job = self.client.query(query)
        job.result()
        logger.info("✅ Created ML features view")

    def create_all_views(self):
        """Create all views for analytics and ML"""
        self.create_fraud_analysis_view()
        self.create_ml_features_view()
        logger.info("✅ Created all views")


def main():
    """Main function to run all transformations"""
    transformer = BigQueryTransformations()
    transformer.create_marts()
    transformer.create_all_views()
    logger.info("✅ All BigQuery transformations completed")

if __name__ == "__main__":
    main()
