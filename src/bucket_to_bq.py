import os
from google.cloud import bigquery
from pyspark.sql import SparkSession
from src.config import BIGQUERY

# Config variables
GCP_PROJECT_ID = BIGQUERY["PROJECT_ID"]
BQ_DATASET = BIGQUERY["DATASET"]
BQ_TABLE = BIGQUERY["TABLE"]
GCS_URI = BIGQUERY["GCS_URI"]  # e.g. gs://bucket/topics/transactions/year=*/month=*/day=*/hour=*/*.json
PARQUET_OUTPUT_BUCKET = BIGQUERY["PARQUET_OUTPUT_BUCKET"]
PARQUET_OUTPUT_PREFIX = BIGQUERY["PARQUET_OUTPUT_PREFIX"]  # e.g. clean/transactions/

def load_parquet_to_bigquery(gcs_parquet_uri):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    load_job = client.load_table_from_uri(
        gcs_parquet_uri,
        table_id,
        job_config=job_config
    )
    load_job.result()
    print(f"✅ Loaded Parquet file {gcs_parquet_uri} into BigQuery table {table_id}")

def main():
    print("🚀 Starting PySpark transformation pipeline...")

    spark = (
        SparkSession.builder
        .appName("KafkaGCS_JSON_to_Parquet")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/keys/key.json")
        .getOrCreate()
    )

    print(f"📂 Reading JSON files from: {GCS_URI}")
    df = spark.read.option("multiLine", False).json(GCS_URI)

    print("📊 Schema:")
    df.printSchema()

    parquet_output_path = f"gs://{PARQUET_OUTPUT_BUCKET}/{PARQUET_OUTPUT_PREFIX}"
    print(f"📦 Writing transformed data to Parquet at: {parquet_output_path}")
    df.write.mode("overwrite").parquet(parquet_output_path)

    print("📤 Loading to BigQuery...")
    load_parquet_to_bigquery(parquet_output_path)

    spark.stop()
    print("✅ ETL pipeline complete.")

if __name__ == "__main__":
    main()