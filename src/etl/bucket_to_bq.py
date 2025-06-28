import os
import logging
from google.cloud import bigquery
from google.cloud import storage
from src.utils.config import BIGQUERY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config variables
GCP_PROJECT_ID = BIGQUERY["PROJECT_ID"]
BQ_DATASET = BIGQUERY["DATASET"]
BQ_RAW_TABLE = f"{BIGQUERY.get('TABLE', 'raw_data')}_raw"
BQ_TRANSFORMED_TABLE = BIGQUERY.get('TABLE', 'transactions')
GCS_URI = BIGQUERY["GCS_URI"]  # e.g. gs://bucket/topics/transactions/year=*/month=*/day=*/hour=*/*.json
BUCKET_NAME = BIGQUERY["PARQUET_OUTPUT_BUCKET"]

def load_data_to_bigquery_raw(gcs_uri):
    """Load data directly from GCS to BigQuery raw table without transformation."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    raw_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
    
    # Get the latest timestamp from BigQuery
    query = f"""
        SELECT MAX(timestamp) as last_load_time 
        FROM `{raw_table_id}`
    """
    last_load_time = None
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            last_load_time = row.last_load_time
    except Exception as e:
        logger.warning(f"No existing data found: {str(e)}")
    
    # Configure the load job with partitioning
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  # Changed from JSON to PARQUET
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"  # Assuming your data has a timestamp field
        ),
        clustering_fields=["user_id"]  # Optional clustering
    )
    
    # Load data from GCS
    load_job = client.load_table_from_uri(
        gcs_uri,
        raw_table_id,
        job_config=job_config
    )
    
    # Wait for the job to complete
    load_job.result()
    
    logger.info(f"✅ Loaded data to BigQuery raw table {raw_table_id}")
    
    return raw_table_id

def transform_data_in_bigquery(raw_table_id):
    """Transform data in BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    transformed_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TRANSFORMED_TABLE}"
    
    # Define the transformation query
    query = f"""
        SELECT 
            -- Add transformation logic here
            *
        FROM 
            `{raw_table_id}`
    """
    
    # Configure the query job
    job_config = bigquery.QueryJobConfig(
        destination=transformed_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    # Start the query job
    query_job = client.query(query, job_config=job_config)
    
    # Wait for the job to complete
    query_job.result()
    logger.info(f"✅ Transformed data in BigQuery table {transformed_table_id}")
    
    return transformed_table_id

def main():
    logger.info("🚀 Starting ELT pipeline...")
    
    # Load data to BigQuery raw table
    raw_table_id = load_data_to_bigquery_raw(GCS_URI)
    
    # Transform data in BigQuery
    transformed_table_id = transform_data_in_bigquery(raw_table_id)
    
    logger.info("✅ ELT pipeline complete.")


if __name__ == "__main__":
    main()