"""
Airflow DAG for registering schemas and running the financial transaction pipeline.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 12),
}

# Define the DAG
dag = DAG(
    'financial_transactions_schema_dag',
    default_args=default_args,
    description='DAG for registering schemas and running financial transaction processes',
    schedule_interval=None,
)

# Define a function to run Python scripts
def run_python_script(script_name):
    subprocess.run(['python', script_name])

# Define task for schema registration
task_register_schemas = PythonOperator(
    task_id='register_schemas',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/scripts/register_all_schemas.py'],
    dag=dag,
)

# Define tasks for producers
task_merchant_producer = PythonOperator(
    task_id='merchant_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producers/merchant_producer.py'],
    dag=dag,
)

task_user_producer = PythonOperator(
    task_id='user_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producers/user_producer.py'],
    dag=dag,
)

task_transaction_producer = PythonOperator(
    task_id='transaction_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producers/transaction_producer.py'],
    dag=dag,
)

# Define task for exchange rate service
task_fetch_exchange_rate = PythonOperator(
    task_id='fetch_exchange_rate',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/services/exchange_rate_service.py'],
    dag=dag,
)

# Define task for ETL process
task_bucket_to_bq = PythonOperator(
    task_id='bucket_to_bq',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/etl/bucket_to_bq.py'],
    dag=dag,
)

# Set the schedule for tasks running every minute
for task in [task_merchant_producer, task_user_producer, task_transaction_producer]:
    task.schedule_interval = timedelta(minutes=1)

# Set the schedule for FetchExchangeRate.py to run once every 24 hours
task_fetch_exchange_rate.schedule_interval = timedelta(days=1)

# Set the schedule for bucket_to_bq to run every hour
task_bucket_to_bq.schedule_interval = timedelta(hours=1)

# Set task dependencies
task_register_schemas >> [task_merchant_producer, task_user_producer]
task_merchant_producer >> task_transaction_producer
task_user_producer >> task_transaction_producer
task_transaction_producer >> task_bucket_to_bq

# Exchange rate service runs independently
task_register_schemas >> task_fetch_exchange_rate
