from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 12),  # Set the start date for the DAG
}

# Define the DAG
dag = DAG(
    'financial_transactions_dag',
    default_args=default_args,
    description='DAG for running financial transaction processes',
    schedule_interval=None,  # We'll use individual schedules for each task
)

# Define a function to run Python scripts
def run_python_script(script_name):
    subprocess.run(['python', script_name])

# Define tasks for files that run every minute
task_merchant_producer = PythonOperator(
    task_id='merchant_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producer/merchant_producer.py'],
    dag=dag,
)

task_user_producer = PythonOperator(
    task_id='user_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producer/user_producer.py'],
    dag=dag,
)

task_fraud_blacklist = PythonOperator(
    task_id='fraud_blacklist',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/consumer/FraudBlacklist.py'],
    dag=dag,
)
task_transaction_producer = PythonOperator(
    task_id='transaction_producer',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producer/TransactionProducer.py'],
    dag=dag,
)


# Define task for FetchExchangeRate.py (runs once every 24 hours)
task_fetch_exchange_rate = PythonOperator(
    task_id='fetch_exchange_rate',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/producer/services/FetchExchangeRate.py'],
    dag=dag,
)

# Define task for FetchExchangeRate.py (runs once every 24 hours)
minIO_to_snowflake = PythonOperator(
    task_id='fetch_exchange_rate',
    python_callable=run_python_script,
    op_args=['/opt/airflow/src/load_from_minio_to_snowflake.py'],
    dag=dag,
)

# Set the schedule for tasks running every minute
for task in [task_merchant_producer, task_user_producer, task_transaction_producer, task_fraud_blacklist, minIO_to_snowflake]:
    task.schedule_interval = timedelta(minutes=1)

# Set the schedule for FetchExchangeRate.py to run once every 24 hours
task_fetch_exchange_rate.schedule_interval = timedelta(days=1)

# Set task dependencies if needed (e.g., sequential or parallel execution)
task_merchant_producer >> task_user_producer >> task_transaction_producer >> task_fraud_blacklist >> minIO_to_snowflake
task_fetch_exchange_rate 
