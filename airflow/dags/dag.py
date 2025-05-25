# /home/nicolas/Escritorio/workshops_ETL/workshop_3/airflow/dags/dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from task import (
    task_extract_data,
    task_transform_data,
    task_merge_split_data,
    task_load_individual_cleaned,
    task_train_model,
    task_kafka_producer,
    task_load_full_merged
)

default_args = {
    'depends_on_past': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='happiness_etl_ml_pipeline',
    default_args=default_args,
    description='Pipeline ETL y ML para el dataset de felicidad mundial, con streaming a Kafka.',
    schedule_interval=None, 
    catchup=False, 
    tags=['etl', 'ml', 'kafka', 'happiness'],
) as dag:

    extract_task = PythonOperator(
        task_id='1_extraccion',
        python_callable=task_extract_data,
    )

    transform_task = PythonOperator(
        task_id='2_transformacion',
        python_callable=task_transform_data,
    )

    merge_split_task = PythonOperator(
        task_id='3_1_merge_split_datos',
        python_callable=task_merge_split_data,
    )

    load_individual_task = PythonOperator(
        task_id='3_2_carga_individual_limpios',
        python_callable=task_load_individual_cleaned,
        op_kwargs={'processed_data_path': '/home/nicolas/Escritorio/workshopsETL/workshop_3/data/processed/'}
    )

    train_model_task = PythonOperator(
        task_id='4_1_entrenamiento_modelo',
        python_callable=task_train_model,
        op_kwargs={'models_output_path': '/home/nicolas/Escritorio/workshopsETL/workshop_3/models/'}
    )

    kafka_producer_task = PythonOperator(
        task_id='4_2_kafka_producer',
        python_callable=task_kafka_producer,
        op_kwargs={
            'bootstrap_servers': 'localhost:29092',
            'topic_name': 'happiness_data_to_predict'
        }
    )

    load_full_merged_task = PythonOperator(
        task_id='4_3_carga_merge_completo',
        python_callable=task_load_full_merged,
        op_kwargs={'processed_data_path': '/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/processed/'}
    )


    extract_task >> transform_task
    
    transform_task >> load_individual_task 
    transform_task >> merge_split_task     

    merge_split_task >> train_model_task   
    merge_split_task >> kafka_producer_task   
    merge_split_task >> load_full_merged_task 