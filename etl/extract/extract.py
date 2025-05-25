# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/extract/extract.py

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def load_single_raw_csv(file_path, year_for_log):
    """
    Carga un único archivo CSV crudo.
    Maneja errores de carga.
    """
    logger.info(f"Intentando cargar el archivo CSV crudo para el año {year_for_log}: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Archivo {os.path.basename(file_path)} ({year_for_log}) cargado exitosamente. Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
        return df
    except FileNotFoundError:
        logger.error(f"Error Crítico: El archivo crudo {file_path} (año {year_for_log}) no fue encontrado.")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"Error Crítico: El archivo crudo {file_path} (año {year_for_log}) está vacío.")
        raise
    except pd.errors.ParserError:
        logger.error(f"Error Crítico: No se pudo parsear el archivo crudo {file_path} (año {year_for_log}). Verifica el formato del CSV.")
        raise
    except Exception as e:
        logger.error(f"Error Crítico: Ocurrió un error inesperado al cargar {file_path} (año {year_for_log}): {e}")
        raise

def extract_all_raw_data(raw_data_path_base="/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/raw/"):
    """
    Carga los 5 datasets de felicidad crudos (2015-2019).
    Retorna una tupla de 5 DataFrames (df_2015, df_2016, df_2017, df_2018, df_2019).
    Lanza una excepción si algún archivo no puede ser cargado.
    """
    logger.info(f"Iniciando extracción de todos los datasets crudos desde: {raw_data_path_base}")
    
    years = [2015, 2016, 2017, 2018, 2019]
    dataframes_raw = {}
    
    for year in years:
        file_name = f"{year}.csv"
        file_path = os.path.join(raw_data_path_base, file_name)
        if year == 2018:
            logger.info(f"Aplicando manejo especial de 'N/A' para {file_name}")
            try:
                df = pd.read_csv(file_path, na_values=['N/A'])
                logger.info(f"Archivo {file_name} ({year}) cargado con manejo de N/A. Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
                dataframes_raw[year] = df
            except Exception as e:
                logger.error(f"Error cargando {file_name} con manejo de N/A: {e}")
                raise
        else:
            dataframes_raw[year] = load_single_raw_csv(file_path, year)
            
    if len(dataframes_raw) == len(years):
        logger.info("Todos los datasets crudos fueron cargados exitosamente.")
        return (
            dataframes_raw[2015],
            dataframes_raw[2016],
            dataframes_raw[2017],
            dataframes_raw[2018],
            dataframes_raw[2019]
        )
    else:
        logger.error("No todos los datasets crudos pudieron ser cargados.")
        raise RuntimeError("Fallo en la carga de uno o más datasets crudos.")

if __name__ == '__main__':
    logger.info("Ejecutando extract.py como script independiente para pruebas.")
    
    test_raw_data_path = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/raw/"
    
    try:
        df_2015, df_2016, df_2017, df_2018, df_2019 = extract_all_raw_data(test_raw_data_path)
        
        logger.info("Prueba de extracción completada. Resumen de los DataFrames:")
        if df_2015 is not None: logger.info(f"df_2015: {df_2015.shape}, Columnas: {df_2015.columns.tolist()}")
        if df_2016 is not None: logger.info(f"df_2016: {df_2016.shape}, Columnas: {df_2016.columns.tolist()}")
        if df_2017 is not None: logger.info(f"df_2017: {df_2017.shape}, Columnas: {df_2017.columns.tolist()}")
        if df_2018 is not None: logger.info(f"df_2018: {df_2018.shape}, Columnas: {df_2018.columns.tolist()}")
        if df_2019 is not None: logger.info(f"df_2019: {df_2019.shape}, Columnas: {df_2019.columns.tolist()}")
        
        if df_2018 is not None:
            print("\nHead de df_2018 (prueba):")
            print(df_2018.head())
            print("\nInfo de df_2018 (prueba):")
            df_2018.info()
            
    except Exception as e:
        logger.error(f"Error durante la prueba del script de extracción: {e}")