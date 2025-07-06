from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.services.exchange_rate_service import ExchangeRateService

def _fetch_exchange_rates():
    service = ExchangeRateService()
    service.run()

with DAG(
    dag_id='fetch_exchange_rate_dag',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['exchange_rate', 'kafka'],
) as dag:
    fetch_exchange_rates_task = PythonOperator(
        task_id='fetch_exchange_rates',
        python_callable=_fetch_exchange_rates,
    )

