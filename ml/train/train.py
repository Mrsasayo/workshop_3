# /home/nicolas/Escritorio/workshops_ETL/workshop_3/ml/train/train.py

import pandas as pd
import numpy as np
import joblib
import os
import logging

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

GLOBAL_RANDOM_STATE_MODEL_TRAIN = 42

def train_and_save_model(df_train, models_output_path, model_filename="trained_happiness_model_pipeline.joblib"):
    """
    Entrena el modelo GradientBoostingRegressor con el preprocesamiento del Escenario S1
    y los hiperparámetros óptimos, y guarda el pipeline entrenado.

    Args:
        df_train (pd.DataFrame): DataFrame de entrenamiento.
        models_output_path (str): Directorio donde se guardará el modelo.
        model_filename (str): Nombre del archivo para el modelo guardado.

    Returns:
        str: Ruta completa al archivo del modelo guardado, o None si falla.
    """
    logger.info(f"Iniciando entrenamiento del modelo con {len(df_train)} filas de datos.")
    if df_train is None or df_train.empty:
        logger.error("El DataFrame de entrenamiento está vacío o es None.")
        return None

    try:
        target_column = 'happiness_score'
        if target_column not in df_train.columns:
            logger.error(f"Columna target '{target_column}' no encontrada en df_train.")
            return None
        
        y_train_model = df_train[target_column]
        features_to_use = [col for col in df_train.columns if col not in [target_column, 'country', 'happiness_rank']]
        X_train_model = df_train[features_to_use]
        
        logger.info(f"Features para el entrenamiento: {X_train_model.columns.tolist()}")

        numeric_features_s1 = X_train_model.select_dtypes(include=[np.number]).columns.tolist()
        categorical_features_s1 = ['region'] if 'region' in X_train_model.columns else []

        numeric_transformer = Pipeline(steps=[('scaler', StandardScaler())])
        categorical_transformer = Pipeline(steps=[
            ('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=False))
        ])

        transformers_list = []
        if numeric_features_s1:
            transformers_list.append(('num', numeric_transformer, numeric_features_s1))
        if categorical_features_s1:
            transformers_list.append(('cat', categorical_transformer, categorical_features_s1))
        
        if not transformers_list:
            logger.error("No se definieron transformadores para el preprocesador.")
            return None
            
        preprocessor_s1_final = ColumnTransformer(
            transformers=transformers_list,
            remainder='drop' 
        )
        logger.info("Preprocesador S1 definido para el pipeline de entrenamiento.")

        best_params_gb_s1 = {
            'learning_rate': 0.05,
            'max_depth': 5,
            'n_estimators': 200,
            'subsample': 0.7,
            'random_state': GLOBAL_RANDOM_STATE_MODEL_TRAIN
        }
        gbr_model_final = GradientBoostingRegressor(**best_params_gb_s1)
        logger.info(f"Modelo GradientBoostingRegressor definido con parámetros: {best_params_gb_s1}")

        final_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor_s1_final),
            ('regressor', gbr_model_final)
        ])
        logger.info("Pipeline de preprocesamiento y modelo creado.")

        logger.info("Iniciando entrenamiento del pipeline final...")
        final_pipeline.fit(X_train_model, y_train_model)
        logger.info("Pipeline final entrenado exitosamente.")

        os.makedirs(models_output_path, exist_ok=True)
        model_filepath = os.path.join(models_output_path, model_filename)
        joblib.dump(final_pipeline, model_filepath)
        logger.info(f"Pipeline entrenado guardado en: {model_filepath}")
        
        return model_filepath

    except Exception as e:
        logger.error(f"Error durante el entrenamiento o guardado del modelo: {e}", exc_info=True)
        return None

if __name__ == '__main__':
    logger.info("Ejecutando train.py como script independiente para pruebas.")
    
    data_train_dummy = {
        'year': [2015, 2016, 2017, 2018, 2019] * 20, # 100 filas
        'region': ['Region A', 'Region B'] * 50,
        'country': [f'Country_{i}' for i in range(100)],
        'happiness_rank': np.random.randint(1, 150, 100),
        'happiness_score': np.random.rand(100) * 5 + 3, # Scores entre 3 y 8
        'social_support': np.random.rand(100) * 1.5,
        'health_life_expectancy': np.random.rand(100) * 1,
        'generosity': np.random.rand(100) * 0.5,
        'freedom_to_make_life_choices': np.random.rand(100) * 0.6,
        'economy_gdp_per_capita': np.random.rand(100) * 1.8,
        'perceptions_of_corruption': np.random.rand(100) * 0.4
    }
    df_train_for_test = pd.DataFrame(data_train_dummy)
    
    test_models_output_dir = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/models_test/"
    
    try:
        saved_model_path = train_and_save_model(df_train_for_test, test_models_output_dir, "test_gbr_s1_pipeline.joblib")
        if saved_model_path:
            logger.info(f"Prueba de entrenamiento completada. Modelo guardado en: {saved_model_path}")
            # Intentar cargar para verificar
            loaded_model = joblib.load(saved_model_path)
            logger.info(f"Modelo de prueba cargado exitosamente: {type(loaded_model)}")
        else:
            logger.error("La prueba de entrenamiento falló, no se guardó el modelo.")
            
    except Exception as e:
        logger.error(f"Error durante la prueba del script de entrenamiento: {e}", exc_info=True)