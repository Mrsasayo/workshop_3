# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/load/save_transformed_datasets.py

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

def save_single_cleaned_df(df, year, base_output_path):
    """
    Guarda un único DataFrame limpio en un archivo CSV.
    """
    if df is None:
        logger.warning(f"No se proporcionó DataFrame para el año {year}. No se guardará nada.")
        return f"No se guardó DataFrame para el año {year} (datos nulos)."

    file_name = f"{year}_cleaned.csv"
    output_filepath = os.path.join(base_output_path, file_name)
    
    try:
        os.makedirs(base_output_path, exist_ok=True)
        
        df.to_csv(output_filepath, index=False)
        logger.info(f"DataFrame limpio para el año {year} guardado exitosamente en: {output_filepath}")
        return f"DataFrame para {year} guardado en {output_filepath}"
    except Exception as e:
        logger.error(f"Error al guardar el DataFrame para el año {year} en '{output_filepath}': {e}")
        return f"Error guardando DataFrame para {year}: {e}"


def save_transformed_datasets(dataframes_cleaned_tuple, processed_data_path):
    """
    Guarda los 5 DataFrames limpios y transformados en archivos CSV individuales.
    
    Args:
        dataframes_cleaned_tuple (tuple): Tupla de 5 DataFrames 
                                          (df_2015_c, df_2016_c, ..., df_2019_c).
        processed_data_path (str): Ruta al directorio donde se guardarán los archivos.
        
    Returns:
        str: Un mensaje de estado.
    """
    logger.info(f"Iniciando guardado de DataFrames limpios individuales en: {processed_data_path}")

    if dataframes_cleaned_tuple is None or len(dataframes_cleaned_tuple) != 5:
        logger.error("Se esperaba una tupla de 5 DataFrames limpios, pero se recibió algo diferente.")
        return "Error: No se recibieron los DataFrames correctos para guardar."

    df_2015_c, df_2016_c, df_2017_c, df_2018_c, df_2019_c = dataframes_cleaned_tuple
    
    years_dfs = {
        2015: df_2015_c,
        2016: df_2016_c,
        2017: df_2017_c,
        2018: df_2018_c,
        2019: df_2019_c
    }
    
    results = []
    for year, df_to_save in years_dfs.items():
        result = save_single_cleaned_df(df_to_save, year, processed_data_path)
        results.append(result)
        
    logger.info("Proceso de guardado de DataFrames individuales completado.")
    return "Resultados del guardado individual: " + "; ".join(results)


if __name__ == '__main__':
    logger.info("Ejecutando save_transformed_datasets.py como script independiente para pruebas.")
    
    dummy_df_2015 = pd.DataFrame({'year': [2015], 'country': ['Testland15'], 'happiness_score': [7.0]})
    dummy_df_2016 = pd.DataFrame({'year': [2016], 'country': ['Testland16'], 'happiness_score': [7.1]})
    dummy_df_2017 = pd.DataFrame({'year': [2017], 'country': ['Testland17'], 'happiness_score': [7.2]})
    dummy_df_2018 = pd.DataFrame({'year': [2018], 'country': ['Testland18'], 'happiness_score': [7.3]})
    dummy_df_2019 = pd.DataFrame({'year': [2019], 'country': ['Testland19'], 'happiness_score': [7.4]})
    
    dfs_tuple_test = (dummy_df_2015, dummy_df_2016, dummy_df_2017, dummy_df_2018, dummy_df_2019)
    
    test_output_path = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/processed_test/"
    
    try:
        status_message = save_transformed_datasets(dfs_tuple_test, test_output_path)
        logger.info(f"Prueba de guardado completada. Mensaje: {status_message}")
        for year_test in [2015, 2016, 2017, 2018, 2019]:
            expected_file = os.path.join(test_output_path, f"{year_test}_cleaned.csv")
            if os.path.exists(expected_file):
                logger.info(f"Archivo de prueba {expected_file} creado exitosamente.")
            else:
                logger.error(f"Archivo de prueba {expected_file} NO fue creado.")
                
    except Exception as e:
        logger.error(f"Error durante la prueba del script de guardado: {e}")