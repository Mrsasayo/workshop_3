# /home/nicolas/Escritorio/workshops ETL/workshop_3/ml/predict/predict_consumer.py

import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
from psycopg2 import extras
import logging
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger("kafka_consumer_predictor")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root_dir = os.path.dirname(os.path.dirname(script_dir))
env_path = os.path.join(project_root_dir, 'config', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"Variables de entorno cargadas desde: {env_path}")
else:
    logger.warning(f"Archivo .env no encontrado en {env_path}.")

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_PREDICT_TOPIC', 'happiness_data_to_predict')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP', 'happiness_predictor_group_main') 
MODEL_FILENAME = os.getenv('MODEL_FILENAME', 'trained_happiness_model_pipeline.joblib')
MODEL_PATH = os.path.join(project_root_dir, 'models', MODEL_FILENAME)
CSV_OUTPUT_PATH = os.path.join(project_root_dir, 'data', 'predictions_output.csv')
CONSUMER_RUN_DURATION_MINUTES = 5

PG_HOST = os.getenv('POSTGRES_HOST', 'localhost')
PG_PORT = os.getenv('POSTGRES_PORT', '5432')
PG_DATABASE = os.getenv('POSTGRES_DATABASE', 'mrsasayo')
PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
PG_TABLE_NAME = os.getenv('PG_PREDICTIONS_TABLE', 'happiness_predictions')

EXPECTED_DF_COLUMNS = [
    'year', 'region', 'country', 'happiness_rank', 'happiness_score', 
    'social_support', 'health_life_expectancy', 'generosity',
    'freedom_to_make_life_choices', 'economy_gdp_per_capita',
    'perceptions_of_corruption', 'predicted_happiness_score' 
]


def load_prediction_model(path_to_model):
    try:
        model_pipeline = joblib.load(path_to_model)
        logger.info(f"Modelo cargado exitosamente desde: {path_to_model}")
        return model_pipeline
    except FileNotFoundError:
        logger.error(f"Error Crítico: Archivo del modelo no encontrado en {path_to_model}")
    except Exception as e:
        logger.error(f"Error Crítico al cargar el modelo desde {path_to_model}: {e}")
    return None

def create_kafka_consumer(topic, bootstrap_servers, group_id):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest', 
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=1000 
        )
        logger.info(f"Consumidor conectado al topic '{topic}' en Kafka: {bootstrap_servers}")
        return consumer
    except NoBrokersAvailable:
        logger.error(f"No se pudo conectar a ningún broker de Kafka en {bootstrap_servers}. Verifica que Kafka esté corriendo.")
    except Exception as e:
        logger.error(f"Error al crear consumidor de Kafka: {e}")
    return None


def get_postgres_connection():
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        logger.info(f"Conectado exitosamente a PostgreSQL DB: {PG_DATABASE} en {PG_HOST}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error al conectar a PostgreSQL: {e}")
    return None

def create_predictions_table_if_not_exists(conn):
    if conn is None: return
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {PG_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        year INT,
        region VARCHAR(255),
        country VARCHAR(255),
        happiness_rank INT,
        happiness_score FLOAT, 
        social_support FLOAT,
        health_life_expectancy FLOAT,
        generosity FLOAT,
        freedom_to_make_life_choices FLOAT,
        economy_gdp_per_capita FLOAT,
        perceptions_of_corruption FLOAT,
        predicted_happiness_score FLOAT,
        prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        logger.info(f"Tabla '{PG_TABLE_NAME}' asegurada/creada en PostgreSQL.")
    except psycopg2.Error as e:
        logger.error(f"Error al crear/asegurar la tabla '{PG_TABLE_NAME}': {e}")
        conn.rollback()

def save_prediction_to_postgres(conn, prediction_data):
    if conn is None: return False
    
    cols_for_insert_order = [
        'year', 'country', 'region', 'happiness_rank', 'happiness_score',
        'social_support', 'health_life_expectancy', 'generosity',
        'freedom_to_make_life_choices', 'economy_gdp_per_capita',
        'perceptions_of_corruption', 'predicted_happiness_score'
    ]
    
    values_tuple = tuple(prediction_data.get(col) for col in cols_for_insert_order)
    
    insert_query = f"""
    INSERT INTO {PG_TABLE_NAME} 
    (year, country, region, happiness_rank, happiness_score, social_support, health_life_expectancy, 
    generosity, freedom_to_make_life_choices, economy_gdp_per_capita, perceptions_of_corruption, 
    predicted_happiness_score)
    VALUES ({', '.join(['%s'] * len(cols_for_insert_order))});
    """
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, values_tuple)
            conn.commit()
        logger.info(f"Predicción para '{prediction_data.get('country')}' guardada en PostgreSQL.")
        return True
    except psycopg2.Error as e:
        logger.error(f"Error al insertar predicción en PostgreSQL: {e}")
        conn.rollback()
    return False


def main():
    logger.info(f"Iniciando consumidor. Escuchará durante {CONSUMER_RUN_DURATION_MINUTES} minutos.")
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=CONSUMER_RUN_DURATION_MINUTES)

    model = load_prediction_model(MODEL_PATH)
    if model is None: return

    pg_conn = get_postgres_connection()
    if pg_conn is None: return
    create_predictions_table_if_not_exists(pg_conn)

    consumer = create_kafka_consumer(KAFKA_TOPIC_NAME, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP_ID)
    if consumer is None:
        if pg_conn: pg_conn.close()
        return

    all_predictions_for_csv = [] 
    messages_processed_total = 0
    last_message_time = time.time() 
    try:
        while datetime.now() < end_time:
            logger.debug("Sondeando mensajes...")
            for message in consumer:
                if message is None:
                    if datetime.now() >= end_time: break
                    continue

                messages_processed_total += 1
                data_to_predict_dict = message.value 
                logger.info(f"Mensaje {messages_processed_total} (offset={message.offset}) recibido: {data_to_predict_dict}")

                excluded_columns = ['predicted_happiness_score','happiness_rank','happiness_score']
                identifier_columns = [col for col in EXPECTED_DF_COLUMNS if col not in excluded_columns]
                
                conditions = []
                values_for_conditions = []
                valid_record_for_check = True
                for col in identifier_columns:
                    if col in data_to_predict_dict and data_to_predict_dict[col] is not None:
                        conditions.append(f"{col} = %s")
                        values_for_conditions.append(data_to_predict_dict[col])
                    else:
                        logger.warning(f"Columna identificadora '{col}' es None o no está en el mensaje. Saltando chequeo de duplicados para este mensaje.")
                        valid_record_for_check = False
                        break 
                
                if pg_conn and valid_record_for_check:
                    try:
                        with pg_conn.cursor() as cur:
                            check_query = f"SELECT 1 FROM {PG_TABLE_NAME} WHERE {' AND '.join(conditions)} LIMIT 1;"
                            cur.execute(check_query, tuple(values_for_conditions))
                            exists = cur.fetchone()
                            
                            if exists:
                                logger.info(f"Predicción para { {k: data_to_predict_dict.get(k) for k in ['year', 'country']} } ya existe en la base de datos. Saltando.")
                                continue
                    except psycopg2.Error as e_check:
                        logger.error(f"Error al verificar duplicados en PostgreSQL: {e_check}")
                    except Exception as e_general_check:
                        logger.error(f"Error general durante la verificación de duplicados: {e_general_check}")


                try:
                    df_to_predict = pd.DataFrame([data_to_predict_dict])
                    
                    prediction = model.predict(df_to_predict)
                    predicted_score = float(prediction[0]) 
                    
                    prediction_record = data_to_predict_dict.copy()
                    prediction_record['predicted_happiness_score'] = predicted_score
                    
                    save_prediction_to_postgres(pg_conn, prediction_record)
                    
                    record_for_csv = {col: prediction_record.get(col) for col in EXPECTED_DF_COLUMNS}
                    all_predictions_for_csv.append(record_for_csv)
                    
                    current_time = time.time()
                    time_to_wait = 0.5 - (current_time - last_message_time)
                    if time_to_wait > 0:
                        time.sleep(time_to_wait)
                    last_message_time = time.time() 

                except Exception as e_pred:
                    logger.error(f"Error al procesar mensaje o hacer predicción: {e_pred}", exc_info=True)
                
                if datetime.now() >= end_time:
                    logger.info("Tiempo de ejecución del consumidor completado.")
                    break
            
            if datetime.now() >= end_time and messages_processed_total > 0: 
                 break 
            elif datetime.now() >= end_time and messages_processed_total == 0:
                 logger.info("Tiempo de ejecución completado, no se procesaron mensajes en este ciclo.")
                 break

    except KeyboardInterrupt:
        logger.info("Consumidor detenido por el usuario.")
    except Exception as e:
        logger.error(f"Error inesperado en el bucle del consumidor: {e}", exc_info=True)
    finally:
        if consumer:
            logger.info("Cerrando consumidor de Kafka.")
            consumer.close()
        if pg_conn:
            logger.info("Cerrando conexión a PostgreSQL.")
            pg_conn.close()
        
        if all_predictions_for_csv:
            try:
                predictions_df = pd.DataFrame(all_predictions_for_csv)
                predictions_df = predictions_df[EXPECTED_DF_COLUMNS]
                predictions_df.to_csv(CSV_OUTPUT_PATH, index=False)
                logger.info(f"Todas las predicciones procesadas ({len(all_predictions_for_csv)}) guardadas en: {CSV_OUTPUT_PATH}")
            except Exception as e_csv:
                logger.error(f"Error al guardar predicciones en CSV: {e_csv}")
        else:
            logger.info("No se procesaron predicciones para guardar en CSV.")
            try:
                pd.DataFrame(columns=EXPECTED_DF_COLUMNS).to_csv(CSV_OUTPUT_PATH, index=False)
                logger.info(f"Archivo CSV vacío con encabezados creado en: {CSV_OUTPUT_PATH}")
            except Exception as e_empty_csv:
                 logger.error(f"Error al crear archivo CSV vacío: {e_empty_csv}")


    logger.info("Consumidor de predicciones finalizado.")

if __name__ == '__main__':
    main()