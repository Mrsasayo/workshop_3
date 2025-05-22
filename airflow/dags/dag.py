# /home/nicolas/Escritorio/workshops ETL/workshop_3/airflow/dags/dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# Si planeas usar el DockerOperator para Kafka (aunque dijimos que no para la Opción 1)
# from airflow.providers.docker.operators.docker import DockerOperator 

# Importar las funciones de tarea desde task.py
# Asegúrate de que la ruta de importación sea correcta según cómo Airflow vea tu directorio de DAGs.
# Si task.py está en el mismo directorio que dag.py, esto debería funcionar.
from task import (
    task_extract_data,
    task_transform_data,
    task_merge_split_data,
    task_load_individual_cleaned,
    task_train_model,
    task_kafka_producer,
    task_load_full_merged
)

# Argumentos por defecto para el DAG
default_args = {
    'depends_on_past': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

# Definición del DAG
with DAG(
    dag_id='happiness_etl_ml_pipeline',
    default_args=default_args,
    description='Pipeline ETL y ML para el dataset de felicidad mundial, con streaming a Kafka.',
    schedule_interval=None, # Para ejecución manual o disparada externamente por ahora. Podría ser '@daily', etc.
    catchup=False, # Evita que el DAG corra para todas las fechas pasadas desde start_date
    tags=['etl', 'ml', 'kafka', 'happiness'],
) as dag:

    # --- Tarea 1: Extracción ---
    extract_task = PythonOperator(
        task_id='1_extraccion',
        python_callable=task_extract_data,
        # op_kwargs={'raw_data_path': '/home/nicolas/Escritorio/workshops ETL/workshop_3/data/raw/'} # Ejemplo si la función lo necesita
    )

    # --- Tarea 2: Transformación ---
    transform_task = PythonOperator(
        task_id='2_transformacion',
        python_callable=task_transform_data,
        # La salida de extract_task (los 5 DFs) se pasará automáticamente vía XCom
        # a task_transform_data si esta los acepta como argumentos o los lee de XCom.
    )

    # --- Tarea 3.1: Merge y Split ---
    merge_split_task = PythonOperator(
        task_id='3_1_merge_split_datos',
        python_callable=task_merge_split_data,
    )

    # --- Tarea 3.2: Carga Individual Limpios ---
    # Esta tarea toma la salida de la tarea 2 (transform_task)
    load_individual_task = PythonOperator(
        task_id='3_2_carga_individual_limpios',
        python_callable=task_load_individual_cleaned,
        # op_kwargs para pasar la ruta de salida, por ejemplo
        op_kwargs={'processed_data_path': '/home/nicolas/Escritorio/workshops ETL/workshop_3/data/processed/'}
    )

    # --- Tarea 4.1: Entrenamiento del Modelo ---
    # Toma la salida df_train de merge_split_task
    train_model_task = PythonOperator(
        task_id='4_1_entrenamiento_modelo',
        python_callable=task_train_model,
        op_kwargs={'models_output_path': '/home/nicolas/Escritorio/workshops ETL/workshop_3/models/'}
    )

    # --- Tarea 4.2: Kafka Producer ---
    # Toma la salida df_predict_stream de merge_split_task
    kafka_producer_task = PythonOperator(
        task_id='4_2_kafka_producer',
        python_callable=task_kafka_producer,
        # op_kwargs para pasar configuración de Kafka si es necesario (ej. topic, bootstrap_servers)
        op_kwargs={
            'bootstrap_servers': 'localhost:29092', # Asumiendo Kafka en Docker expuesto en este puerto
            'topic_name': 'happiness_data_to_predict'
        }
    )

    # --- Tarea 4.3: Carga Merge Completo ---
    # Toma la salida df_unified_full de merge_split_task
    load_full_merged_task = PythonOperator(
        task_id='4_3_carga_merge_completo',
        python_callable=task_load_full_merged,
        op_kwargs={'processed_data_path': '/home/nicolas/Escritorio/workshops ETL/workshop_3/data/processed/'}
    )

    # --- Definición de Dependencias ---
    # Sintaxis: upstream_task >> downstream_task
    # O: upstream_task.set_downstream(downstream_task)

    extract_task >> transform_task
    
    transform_task >> load_individual_task # 3.2 depende de 2
    transform_task >> merge_split_task     # 3.1 también depende de 2

    merge_split_task >> train_model_task      # 4.1 depende de 3.1
    merge_split_task >> kafka_producer_task   # 4.2 depende de 3.1
    merge_split_task >> load_full_merged_task # 4.3 depende de 3.1