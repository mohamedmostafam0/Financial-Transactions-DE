from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_python_script(script_path):
    subprocess.run(["python", script_path], check=True)

@dag(
    dag_id="financial_realtime_dag",
    description="Runs producer tasks every minute",
    schedule="* * * * *",
    start_date=datetime(2025, 4, 12),
    catchup=False,
    default_args=default_args,
    tags=["etl", "financial"],
)
def financial_dag():
    merchant = PythonOperator(
        task_id="merchant_producer",
        python_callable=run_python_script,
        op_args=["/opt/airflow/src/producer/merchant_producer.py"]
    )
    user = PythonOperator(
        task_id="user_producer",
        python_callable=run_python_script,
        op_args=["/opt/airflow/src/producer/user_producer.py"]
    )
    transaction = PythonOperator(
        task_id="transaction_producer",
        python_callable=run_python_script,
        op_args=["/opt/airflow/src/producer/TransactionProducer.py"]
    )
    blacklist = PythonOperator(
        task_id="fraud_blacklist",
        python_callable=run_python_script,
        op_args=["/opt/airflow/src/consumer/FraudBlacklist.py"]
    )

    merchant >> user >> transaction >> blacklist

# ⬇️ This registers the DAG properly
dag = financial_dag()
