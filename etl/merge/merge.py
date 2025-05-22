# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/merge/merge.py

import pandas as pd
from sklearn.model_selection import train_test_split # Para el split
import logging
import os
import sys


# Configurar logger
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Constante para la reproducibilidad del split
GLOBAL_RANDOM_STATE_FOR_SPLIT = 42 
DESIRED_FINAL_COLUMN_ORDER = [
    'year', 'region', 'country', 'happiness_rank', 'happiness_score',
    'social_support', 'health_life_expectancy', 'generosity',
    'freedom_to_make_life_choices', 'economy_gdp_per_capita',
    'perceptions_of_corruption'
]

def merge_and_split_dataframes(df_2015_cleaned, df_2016_cleaned, df_2017_cleaned, df_2018_cleaned, df_2019_cleaned,
                               test_size=0.2, random_state=GLOBAL_RANDOM_STATE_FOR_SPLIT):
    """
    Concatena los DataFrames limpios y los divide en conjuntos de entrenamiento y predicción/streaming.
    
    Args:
        df_2015_cleaned (pd.DataFrame): DataFrame limpio para 2015.
        ... (similares para 2016 a 2019) ...
        test_size (float): Proporción del dataset a incluir en el conjunto de predicción/streaming.
        random_state (int): Semilla para la reproducibilidad del split.
        
    Returns:
        tuple: (df_train, df_predict_stream, df_unified_full)
    """
    logger.info("Iniciando proceso de merge y split de DataFrames limpios.")

    list_of_cleaned_dfs = []
    if df_2015_cleaned is not None: list_of_cleaned_dfs.append(df_2015_cleaned)
    if df_2016_cleaned is not None: list_of_cleaned_dfs.append(df_2016_cleaned)
    if df_2017_cleaned is not None: list_of_cleaned_dfs.append(df_2017_cleaned)
    if df_2018_cleaned is not None: list_of_cleaned_dfs.append(df_2018_cleaned)
    if df_2019_cleaned is not None: list_of_cleaned_dfs.append(df_2019_cleaned)

    if not list_of_cleaned_dfs:
        logger.error("No se proporcionaron DataFrames limpios para concatenar.")
        raise ValueError("No hay DataFrames para unificar.")

    # 1. Concatenar los DataFrames
    try:
        df_unified_full = pd.concat(list_of_cleaned_dfs, ignore_index=True)
        logger.info(f"DataFrames concatenados. DataFrame unificado tiene {df_unified_full.shape[0]} filas y {df_unified_full.shape[1]} columnas.")
    except Exception as e:
        logger.error(f"Error durante la concatenación de DataFrames: {e}")
        raise

    # 2. Verificar consistencia de columnas (opcional aquí, ya se debería haber hecho)
    #    Pero es bueno asegurarse de que todas las columnas esperadas estén.
    expected_cols_after_clean = set(DESIRED_FINAL_COLUMN_ORDER) # Definido en transform.py o aquí
    actual_cols_unified = set(df_unified_full.columns)
    if not expected_cols_after_clean.issubset(actual_cols_unified): # Chequea si todas las esperadas están
        missing = expected_cols_after_clean - actual_cols_unified
        logger.warning(f"Columnas esperadas faltantes en el DF unificado: {missing}")
        # Podrías lanzar un error si faltan columnas críticas
        
    # 3. Dividir el DataFrame unificado
    #    Asegurarse de que la columna target exista para la estratificación si se usa.
    #    Para un split simple, solo necesitamos X e y.
    if 'happiness_score' not in df_unified_full.columns:
        logger.error("La columna 'happiness_score' (target) no se encuentra en el DataFrame unificado para el split.")
        raise ValueError("Columna target faltante para el split.")

    # Separar features y target para el split (si quieres estratificar por el target)
    # X_unified = df_unified_full.drop(columns=['happiness_score']) # O las features que usarás
    # y_unified = df_unified_full['happiness_score']
    
    # train_test_split divide en train y test (que usaremos como predict_stream)
    # Si el dataset es pequeño, estratificar puede no ser crucial, pero si el target tiene
    # una distribución desigual, puede ser útil. Para regresión, la estratificación es más compleja.
    # Por ahora, un split aleatorio simple.
    
    try:
        df_train, df_predict_stream = train_test_split(
            df_unified_full,
            test_size=test_size,
            random_state=random_state
            # Si quisieras estratificar por 'year' o 'region' (siempre que no haya muchos NaNs):
            # stratify=df_unified_full['year'] if df_unified_full['year'].nunique() > 1 else None 
            # (estratificar por una variable con pocos valores únicos por muestra puede dar error)
        )
        logger.info(f"DataFrame unificado dividido: df_train ({df_train.shape[0]} filas), df_predict_stream ({df_predict_stream.shape[0]} filas).")
    except Exception as e:
        logger.error(f"Error durante el split de datos: {e}")
        raise
        
    return df_train, df_predict_stream, df_unified_full


# --- Bloque para pruebas si se ejecuta el script directamente ---
if __name__ == '__main__':
    logger.info("Ejecutando merge.py como script independiente para pruebas.")
    
    # Para probar, necesitaríamos cargar los DFs limpios (o crear dummies)
    # Asumimos que los archivos _cleaned.csv existen en data/processed/
    base_path_processed_test = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/processed/"
    
    dfs_test_cleaned = []
    loaded_ok = True
    for year_test in [2015, 2016, 2017, 2018, 2019]:
        try:
            path_test = os.path.join(base_path_processed_test, f"{year_test}_cleaned.csv")
            # Simular que DESIRED_FINAL_COLUMN_ORDER está disponible o definirlo aquí para la prueba
            DESIRED_FINAL_COLUMN_ORDER = [
                'year', 'region', 'country', 'happiness_rank', 'happiness_score',
                'social_support', 'health_life_expectancy', 'generosity',
                'freedom_to_make_life_choices', 'economy_gdp_per_capita',
                'perceptions_of_corruption'
            ]
            df_temp = pd.read_csv(path_test)
            # Asegurar que solo tenga las columnas deseadas para simular la salida de transform
            df_temp = df_temp[[col for col in DESIRED_FINAL_COLUMN_ORDER if col in df_temp.columns]]
            dfs_test_cleaned.append(df_temp)
        except FileNotFoundError:
            logger.error(f"Archivo de prueba {path_test} no encontrado.")
            loaded_ok = False
            break
            
    if loaded_ok and len(dfs_test_cleaned) == 5:
        try:
            train_df, predict_df, unified_df = merge_and_split_dataframes(*dfs_test_cleaned)
            logger.info("Prueba de merge y split completada.")
            print(f"df_train shape: {train_df.shape}")
            print(f"df_predict_stream shape: {predict_df.shape}")
            print(f"df_unified_full shape: {unified_df.shape}")
            print("\nHead de df_train (prueba):")
            print(train_df.head(2).to_markdown(index=False))
        except Exception as e:
            logger.error(f"Error durante la prueba del script de merge: {e}")
    else:
        logger.error("No se pudieron cargar todos los DFs limpios para la prueba de merge.")