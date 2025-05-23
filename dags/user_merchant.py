from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 4, 12),
}

def run_script(path):
    subprocess.run(['python', path], check=True)

with DAG(
    dag_id='user_merchant_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    description='Produces user and merchant data',
) as dag:

    user_producer = PythonOperator(
        task_id='user_producer',
        python_callable=run_script,
        op_args=['/opt/airflow/src/producer/user_producer.py']
    )

    merchant_producer = PythonOperator(
        task_id='merchant_producer',
        python_callable=run_script,
        op_args=['/opt/airflow/src/producer/merchant_producer.py']
    )

    user_producer >> merchant_producer
