from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from src.etl.bucket_to_bq import run_bucket_to_bq_etl

with DAG(
    dag_id='bucket_to_bq_dag',
    start_date=days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['gcs', 'bigquery', 'etl'],
) as dag:
    load_gcs_to_bq = PythonOperator(
        task_id='load_gcs_to_bq',
        python_callable=run_bucket_to_bq_etl,
    )
