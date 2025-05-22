# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/load/save_unified_dataset.py

import pandas as pd
import os
import logging

# Configurar logger
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def save_unified_dataframe(df_unified, output_path, filename="happiness_unified_dataset.csv"):
    """
    Guarda el DataFrame unificado completo en un archivo CSV.

    Args:
        df_unified (pd.DataFrame): El DataFrame unificado completo.
        output_path (str): Directorio base donde se guardará el archivo.
        filename (str): Nombre del archivo CSV de salida.

    Returns:
        str: Ruta completa al archivo guardado, o None si falla.
    """
    if df_unified is None or df_unified.empty:
        logger.warning("El DataFrame unificado está vacío o es None. No se guardará nada.")
        return None

    full_filepath = os.path.join(output_path, filename)
    
    try:
        # Asegurarse de que el directorio de salida exista
        os.makedirs(output_path, exist_ok=True)
        
        df_unified.to_csv(full_filepath, index=False)
        logger.info(f"DataFrame unificado completo guardado exitosamente en: {full_filepath}")
        return full_filepath
    except Exception as e:
        logger.error(f"Error al guardar el DataFrame unificado en '{full_filepath}': {e}")
        return None # O re-lanzar la excepción si es crítico


# --- Bloque para pruebas si se ejecuta el script directamente ---
if __name__ == '__main__':
    logger.info("Ejecutando save_unified_dataset.py como script independiente para pruebas.")
    
    # Crear un DataFrame dummy para probar
    data_dummy_unified = {
        'year': [2015, 2016, 2017, 2018, 2019],
        'region': ['Region A', 'Region B', 'Region A', 'Region C', 'Region B'],
        'country': ['C1', 'C2', 'C3', 'C4', 'C5'],
        'happiness_score': [7.0, 7.1, 7.2, 6.9, 7.3],
        # ... (añadir otras columnas estándar si es necesario para la prueba)
        'economy_gdp_per_capita': [1.0, 1.1, 1.2, 0.9, 1.3]
    }
    df_test_unified = pd.DataFrame(data_dummy_unified)
    
    # Columnas estándar esperadas (para asegurar que el dummy tenga una estructura similar)
    DESIRED_FINAL_COLUMN_ORDER_TEST = [
        'year', 'region', 'country', 'happiness_score', 'economy_gdp_per_capita'
        # Añadir el resto si las incluyes en el dummy
    ]
    # Asegurar que el dummy tenga las columnas en el orden esperado (o las que tenga)
    df_test_unified = df_test_unified[[col for col in DESIRED_FINAL_COLUMN_ORDER_TEST if col in df_test_unified.columns]]


    test_output_dir = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/processed_test/" # Usar un directorio de prueba
    
    try:
        saved_path = save_unified_dataframe(df_test_unified, test_output_dir, "test_unified_dataset.csv")
        if saved_path:
            logger.info(f"Prueba de guardado de DF unificado completada. Archivo en: {saved_path}")
            # Verificar que el archivo se haya creado
            if os.path.exists(saved_path):
                logger.info(f"Archivo de prueba {saved_path} creado exitosamente.")
            else:
                logger.error(f"Archivo de prueba {saved_path} NO fue creado.")
        else:
            logger.error("La prueba de guardado de DF unificado falló, no se guardó el archivo.")
            
    except Exception as e:
        logger.error(f"Error durante la prueba del script de guardado unificado: {e}")