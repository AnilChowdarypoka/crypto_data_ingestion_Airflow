from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from data_ingestion import controller

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'crypto_data_fetching_dag',
    default_args=default_args,
    description='Primary DAG for Crypto data pulling',
    schedule_interval=timedelta(minutes=1),
)

run_etl = PythonOperator(
    task_id='crypto_data_fetching_dag',
    python_callable=controller,
    dag=dag,
)

run_etl