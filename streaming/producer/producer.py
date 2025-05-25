# /home/nicolas/Escritorio/workshops_ETL/workshop_3/streaming/producer/producer.py

import pandas as pd
import json 
from kafka import KafkaProducer
from kafka.errors import KafkaError 
import logging
import time 

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def create_kafka_producer(bootstrap_servers_list):
    """
    Crea y retorna una instancia de KafkaProducer.
    Maneja reintentos de conexión.
    """
    producer = None
    retries = 5
    delay = 5
    
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', 
                retries=3 
            )
            logger.info(f"Conectado exitosamente al broker de Kafka en: {bootstrap_servers_list}")
            return producer
        except KafkaError as e:
            logger.warning(f"Error al conectar con Kafka (intento {i+1}/{retries}): {e}. Reintentando en {delay} segundos...")
            if i < retries - 1: 
                time.sleep(delay)
            else:
                logger.error(f"Fallaron todos los intentos de conexión a Kafka en: {bootstrap_servers_list}")
                raise 
    return None


def send_dataframe_to_kafka(df_stream, producer, topic_name):
    """
    Envía cada fila de un DataFrame como un mensaje JSON a un topic de Kafka.
    """
    if df_stream is None or df_stream.empty:
        logger.info("No hay datos en df_stream para enviar a Kafka.")
        return 0 
    
    if producer is None:
        logger.error("Productor de Kafka no está inicializado. No se pueden enviar mensajes.")
        return -1

    num_messages_sent = 0
    total_rows = len(df_stream)
    logger.info(f"Iniciando envío de {total_rows} registros al topic de Kafka: {topic_name}")

    for index, row in df_stream.iterrows():
        message = row.to_dict()
        try:
            future = producer.send(topic_name, value=message)
            num_messages_sent += 1
            if num_messages_sent % 50 == 0 or num_messages_sent == total_rows: # Loguear progreso
                 logger.info(f"Enviados {num_messages_sent}/{total_rows} mensajes a {topic_name}...")
        except KafkaError as e:
            logger.error(f"Error al enviar mensaje {index} a Kafka: {e}")
        except Exception as e_gen:
            logger.error(f"Error general al procesar/enviar fila {index}: {e_gen}")

    try:
        producer.flush()
        logger.info(f"Flush completado. Total de {num_messages_sent} mensajes intentados para {topic_name}.")
    except KafkaError as e:
        logger.error(f"Error durante el flush de Kafka: {e}")
        
    return num_messages_sent


def produce_data_to_kafka(df_predict_stream, bootstrap_servers_str, topic_name):
    """
    Función principal para la tarea de Airflow.
    Crea un productor, envía el DataFrame y cierra el productor.
    
    Args:
        df_predict_stream (pd.DataFrame): DataFrame con los datos a enviar.
        bootstrap_servers_str (str): String de servidores bootstrap (ej. 'localhost:29092').
        topic_name (str): Nombre del topic de Kafka.
        
    Returns:
        str: Mensaje de estado.
    """
    logger.info(f"Iniciando la tarea de producción a Kafka para el topic '{topic_name}'.")
    
    if df_predict_stream is None or df_predict_stream.empty:
        msg = "No hay datos para enviar al productor de Kafka."
        logger.warning(msg)
        return msg

    bootstrap_servers_list = [s.strip() for s in bootstrap_servers_str.split(',')]
    producer = None
    
    try:
        producer = create_kafka_producer(bootstrap_servers_list)
        if producer:
            sent_count = send_dataframe_to_kafka(df_predict_stream, producer, topic_name)
            msg = f"Proceso de envío a Kafka completado. {sent_count} mensajes intentados para el topic '{topic_name}'."
            logger.info(msg)
        else:
            msg = "Fallo al crear el productor de Kafka. No se enviaron mensajes."
            logger.error(msg)
            raise RuntimeError(msg) 
            
    except KafkaError as e:
        msg = f"Error de Kafka durante la tarea de producción: {e}"
        logger.error(msg)
        raise
    except Exception as e_gen:
        msg = f"Error general en la tarea de producción a Kafka: {e_gen}"
        logger.error(msg, exc_info=True)
        raise
    finally:
        if producer:
            logger.info("Cerrando productor de Kafka.")
            producer.close()
            logger.info("Productor de Kafka cerrado.")
            
    return msg


if __name__ == '__main__':
    logger.info("Ejecutando producer.py como script independiente para pruebas.")
    
    data_stream_dummy = {
        'year': [2020, 2020],
        'region': ['Test Region A', 'Test Region B'],
        'country': ['Testlandia', 'Examplia'],
        'happiness_score': [7.5, 6.5],
        'economy_gdp_per_capita': [1.5, 1.2]
    }
    df_test_stream = pd.DataFrame(data_stream_dummy)
    
    test_bootstrap_servers = 'localhost:29092'
    test_topic = 'happiness_test_topic'
    
    try:
        status = produce_data_to_kafka(df_test_stream, test_bootstrap_servers, test_topic)
        logger.info(f"Prueba de Kafka producer completada. Estado: {status}")
                
    except Exception as e:
        logger.error(f"Error durante la prueba del script de Kafka producer: {e}", exc_info=True)