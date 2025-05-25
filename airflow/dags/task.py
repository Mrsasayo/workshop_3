# /home/nicolas/Escritorio/workshops_ETL/workshop_3/airflow/dags/task.py

import pandas as pd
import logging
import os
import sys

dags_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dags_dir) 
project_root = os.path.dirname(airflow_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
    logging.info(f"Añadido al sys.path: {project_root}")

try:
    from etl.extract.extract import extract_all_raw_data
    from etl.transform.transform import transform_all_dataframes 
    from etl.merge.merge import merge_and_split_dataframes 
    from etl.load.save_transformed_datasets import save_transformed_datasets
    from etl.load.save_unified_dataset import save_unified_dataframe
    from ml.train.train import train_and_save_model
    from streaming.producer.producer import produce_data_to_kafka 



    logging.info("Módulos de lógica de negocio importados exitosamente.")
except ImportError as e:
    logging.error(f"Error al importar módulos de lógica de negocio: {e}")
    raise


def task_extract_data(**kwargs):
    raw_data_path_base = os.path.join(project_root, 'data', 'raw')
    logging.info(f"TASK: Iniciando extracción desde: {raw_data_path_base}")
    try:
        df15, df16, df17, df18, df19 = extract_all_raw_data(raw_data_path_base)
        logging.info("TASK: Extracción de datos completada.")
        return df15, df16, df17, df18, df19
    except Exception as e:
        logging.error(f"TASK ERROR en task_extract_data: {e}")
        raise

def task_transform_data(**kwargs):
    ti = kwargs['ti']
    pulled_dfs_raw = ti.xcom_pull(task_ids='1_extraccion')
    if pulled_dfs_raw is None or len(pulled_dfs_raw) != 5:
        logging.error("TASK ERROR: No se pudieron obtener los 5 DataFrames crudos de XCom.")
        raise ValueError("Error en XCom pull para transformación.")
    
    logging.info("TASK: Iniciando transformación de datos.")
    df15_c, df16_c, df17_c, df18_c, df19_c = transform_all_dataframes(*pulled_dfs_raw) # Ajustar argumentos
    
    logging.info("TASK: Transformación de datos completada.")
    return df15_c, df16_c, df17_c, df18_c, df19_c

def task_merge_split_data(**kwargs):
    ti = kwargs['ti']
    pulled_dfs_cleaned = ti.xcom_pull(task_ids='2_transformacion')

    if pulled_dfs_cleaned is None or len(pulled_dfs_cleaned) != 5:
        logging.error("TASK ERROR: No se pudieron obtener los 5 DataFrames limpios de XCom para merge/split.")
        raise ValueError("Error en XCom pull para merge/split.")
        
    df15_c, df16_c, df17_c, df18_c, df19_c = pulled_dfs_cleaned
    
    logging.info("TASK: Iniciando merge y split de datos llamando a merge_and_split_dataframes.")
    try:
        df_train, df_predict_stream, df_unified_full = merge_and_split_dataframes(
            df15_c, df16_c, df17_c, df18_c, df19_c
        )
        logging.info("TASK: Merge y split completados.")
        return df_train, df_predict_stream, df_unified_full
    except Exception as e:
        logging.error(f"TASK ERROR en task_merge_split_data: {e}")
        raise

def task_load_individual_cleaned(processed_data_path, **kwargs):
    """
    Tarea para guardar los 5 DFs limpios individualmente.
    Recibe los DFs limpios de la tarea de transformación (como una tupla).
    """
    ti = kwargs['ti']
    pulled_dfs_cleaned_tuple = ti.xcom_pull(task_ids='2_transformacion')

    if pulled_dfs_cleaned_tuple is None or len(pulled_dfs_cleaned_tuple) != 5:
        logging.error("TASK ERROR: No se pudieron obtener los 5 DataFrames limpios para carga individual.")
        raise ValueError("Error en XCom pull para carga individual.")
        
    logging.info(f"TASK: Guardando DataFrames individuales limpios en {processed_data_path}.")
    try:
        result_message = save_transformed_datasets(
            dataframes_cleaned_tuple=pulled_dfs_cleaned_tuple,
            processed_data_path=processed_data_path
        )
        logging.info(f"TASK: Guardado de DFs individuales completado. Mensaje: {result_message}")
        return result_message 
    except Exception as e:
        logging.error(f"TASK ERROR en task_load_individual_cleaned: {e}")
        raise

def task_train_model(models_output_path, **kwargs):
    ti = kwargs['ti']
    pulled_data_from_merge = ti.xcom_pull(task_ids='3_1_merge_split_datos')
    
    if pulled_data_from_merge is None or not isinstance(pulled_data_from_merge, tuple) or len(pulled_data_from_merge) < 1:
        logging.error("TASK ERROR: No se pudo obtener df_train de XCom para entrenamiento.")
        raise ValueError("Formato de XCom inesperado o df_train ausente.")
        
    df_train = pulled_data_from_merge[0] 
    
    if df_train is None or df_train.empty:
        logging.error("TASK ERROR: df_train recibido de XCom está vacío o es None.")
        raise ValueError("df_train vacío recibido para entrenamiento.")

    logging.info(f"TASK: Iniciando entrenamiento de modelo. df_train tiene {len(df_train)} filas. Guardando en {models_output_path}.")
    try:
        model_path = train_and_save_model(
            df_train=df_train,
            models_output_path=models_output_path
        )
        if model_path is None:
            logging.error("TASK ERROR: El entrenamiento del modelo falló o no se guardó el modelo (train_and_save_model retornó None).")
            raise RuntimeError("Fallo en el entrenamiento del modelo.")
            
        logging.info(f"TASK : Modelo entrenado y guardado en {model_path}.")
        return model_path
    except Exception as e:
        logging.error(f"TASK ERROR en task_train_model: {e}")
        raise

def task_kafka_producer(bootstrap_servers, topic_name, **kwargs):
    """
    Tarea para enviar datos al producer de Kafka.
    Recibe df_predict_stream de la tarea de merge/split.
    """
    ti = kwargs['ti']
    pulled_data_from_merge = ti.xcom_pull(task_ids='3_1_merge_split_datos')

    if pulled_data_from_merge is None or not isinstance(pulled_data_from_merge, tuple) or len(pulled_data_from_merge) < 2:
        logging.error("TASK ERROR: No se pudo obtener df_predict_stream de XCom para Kafka.")
        raise ValueError("Formato de XCom inesperado o df_predict_stream ausente.")
        
    df_predict_stream = pulled_data_from_merge[1]
    
    if df_predict_stream is None or df_predict_stream.empty:
        msg = "TASK INFO: df_predict_stream está vacío. No se enviarán datos a Kafka."
        logging.info(msg)
        return msg

    logging.info(f"TASK: Iniciando envío de {len(df_predict_stream)} registros a Kafka topic '{topic_name}'.")
    try:
        result_message = produce_data_to_kafka(
            df_predict_stream=df_predict_stream,
            bootstrap_servers_str=bootstrap_servers,
            topic_name=topic_name 
        )
        logging.info(f"TASK: Envío a Kafka completado. Mensaje: {result_message}")
        return result_message
    except Exception as e:
        logging.error(f"TASK ERROR en task_kafka_producer: {e}")
        raise

def task_load_full_merged(processed_data_path, **kwargs):
    """
    Tarea para guardar el DataFrame unificado completo.
    Recibe df_unified_full de la tarea de merge/split.
    """
    ti = kwargs['ti']
    pulled_data_from_merge = ti.xcom_pull(task_ids='3_1_merge_split_datos')
    
    if pulled_data_from_merge is None or not isinstance(pulled_data_from_merge, tuple) or len(pulled_data_from_merge) < 3:
        logging.error("TASK ERROR: No se pudo obtener df_unified_full de XCom para guardar.")
        raise ValueError("Formato de XCom inesperado o df_unified_full ausente.")
        
    df_unified_full = pulled_data_from_merge[2] 

    if df_unified_full is None or df_unified_full.empty:
        msg = "TASK INFO: df_unified_full está vacío. No se guardará el archivo."
        logging.info(msg)
        return msg 

    logging.info(f"TASK: Guardando DataFrame unificado completo en {processed_data_path}.")
    try:
        saved_file_path = save_unified_dataframe(
            df_unified=df_unified_full,
            output_path=processed_data_path,
            filename="happiness_unified_dataset.csv"
        )
        if saved_file_path:
            result_message = f"DataFrame unificado completo guardado en: {saved_file_path}"
            logging.info(f"TASK: {result_message}")
        else:
            result_message = "TASK ERROR: Fallo al guardar el DataFrame unificado."
            logging.error(result_message)
            raise RuntimeError(result_message)

        return result_message
    except Exception as e:
        logging.error(f"TASK ERROR en task_load_full_merged: {e}")
        raise