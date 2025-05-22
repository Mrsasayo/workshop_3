from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='example_test_dag_simple',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example_test'],
) as dag:
    dummy_task = EmptyOperator(task_id='dummy_start_task')  ps aux | grep gunicorn