# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/transform/transform.py

import pandas as pd
import numpy as np # Para NaN y funciones numéricas si es necesario
import logging
import os
import sys

# Configurar logger para este módulo
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# --- MAPEOS Y CONFIGURACIONES GLOBALES (podrían estar en un archivo de config separado) ---

# Mapeo de columnas estándar finales (las que queremos en TODOS los DFs limpios)
STANDARD_COLUMN_NAMES = {
    'country': 'country',
    'region': 'region',
    'happiness_rank': 'happiness_rank',
    'happiness_score': 'happiness_score',
    'economy_gdp_per_capita': 'economy_gdp_per_capita',
    'social_support': 'social_support', # 'Family' se mapeará a esto
    'health_life_expectancy': 'health_life_expectancy',
    'freedom_to_make_life_choices': 'freedom_to_make_life_choices', # 'Freedom' se mapeará a esto
    'perceptions_of_corruption': 'perceptions_of_corruption', # 'Trust (Government Corruption)' se mapeará a esto
    'generosity': 'generosity',
    'year': 'year' # Se añadirá esta columna
}

# Orden final deseado de las columnas
DESIRED_FINAL_COLUMN_ORDER = [
    'year', 'region', 'country', 'happiness_rank', 'happiness_score',
    'social_support', 'health_life_expectancy', 'generosity',
    'freedom_to_make_life_choices', 'economy_gdp_per_capita',
    'perceptions_of_corruption'
]

# Mapeos de nombres de columna originales a estándar para cada año
COLUMN_MAPPINGS_BY_YEAR = {
    2015: {
        'Country': 'country', 'Region': 'region', 'Happiness Rank': 'happiness_rank',
        'Happiness Score': 'happiness_score', 'Economy (GDP per Capita)': 'economy_gdp_per_capita',
        'Family': 'social_support', 'Health (Life Expectancy)': 'health_life_expectancy',
        'Freedom': 'freedom_to_make_life_choices', 
        'Trust (Government Corruption)': 'perceptions_of_corruption', 'Generosity': 'generosity'
    },
    2016: {
        'Country': 'country', 'Region': 'region', 'Happiness Rank': 'happiness_rank',
        'Happiness Score': 'happiness_score', 'Economy (GDP per Capita)': 'economy_gdp_per_capita',
        'Family': 'social_support', 'Health (Life Expectancy)': 'health_life_expectancy',
        'Freedom': 'freedom_to_make_life_choices',
        'Trust (Government Corruption)': 'perceptions_of_corruption', 'Generosity': 'generosity'
    },
    2017: {
        'Country': 'country', 'Happiness.Rank': 'happiness_rank', 'Happiness.Score': 'happiness_score',
        'Economy..GDP.per.Capita.': 'economy_gdp_per_capita', 'Family': 'social_support',
        'Health..Life.Expectancy.': 'health_life_expectancy', 'Freedom': 'freedom_to_make_life_choices',
        'Trust..Government.Corruption.': 'perceptions_of_corruption', 'Generosity': 'generosity'
    },
    2018: {
        'Country or region': 'country', 'Overall rank': 'happiness_rank', 'Score': 'happiness_score',
        'GDP per capita': 'economy_gdp_per_capita', 'Social support': 'social_support',
        'Healthy life expectancy': 'health_life_expectancy',
        'Freedom to make life choices': 'freedom_to_make_life_choices',
        'Perceptions of corruption': 'perceptions_of_corruption', 'Generosity': 'generosity'
    },
    2019: {
        'Country or region': 'country', 'Overall rank': 'happiness_rank', 'Score': 'happiness_score',
        'GDP per capita': 'economy_gdp_per_capita', 'Social support': 'social_support',
        'Healthy life expectancy': 'health_life_expectancy',
        'Freedom to make life choices': 'freedom_to_make_life_choices',
        'Perceptions of corruption': 'perceptions_of_corruption', 'Generosity': 'generosity'
    }
}

# Columnas a eliminar para cada año (usando nombres originales del CSV)
COLUMNS_TO_DROP_BY_YEAR = {
    2015: ['Standard Error', 'Dystopia Residual'],
    2016: ['Lower Confidence Interval', 'Upper Confidence Interval', 'Dystopia Residual'],
    2017: ['Whisker.high', 'Whisker.low', 'Dystopia.Residual'],
    2018: [], # No se eliminan columnas
    2019: []  # No se eliminan columnas
}

# Mapeos de países a regiones (de tus notebooks de limpieza)
# Debes copiar aquí tus mapeos finales y verificados para 2017, 2018, 2019
COUNTRY_TO_REGION_MAPS = {
    2017: {
        "Western Europe": ['Norway', 'Denmark', 'Iceland', 'Switzerland', 'Finland', 'Netherlands',
                            'Austria', 'Ireland', 'Germany', 'Belgium', 'Luxembourg', 'United Kingdom',
                            'France', 'Spain', 'Italy', 'Portugal', 'Cyprus', 'Malta', 'Greece', 'North Cyprus',
                            'Sweden'], # Añadido
        "North America": ['Canada', 'United States'],
        "Australia and New Zealand": ['Australia', 'New Zealand'],
        "Middle East and Northern Africa": ['United Arab Emirates', 'Israel', 'Saudi Arabia', 'Qatar',
                                             'Bahrain', 'Kuwait', 'Jordan', 'Lebanon', 'Egypt', 'Libya',
                                             'Tunisia', 'Morocco', 'Algeria', 'Sudan', 'Palestinian Territories',
                                             'Syria', 'Armenia', 'Azerbaijan', 'Georgia',
                                             'Turkey', 'Iran', 'Iraq', 'Yemen'], # Añadidos
        "Latin America and Caribbean": ['Costa Rica', 'Chile', 'Argentina', 'Mexico', 'Guatemala', 'Panama',
                                        'Colombia', 'Brazil', 'Uruguay', 'Paraguay', 'Peru', 'Bolivia',
                                        'Ecuador', 'El Salvador', 'Nicaragua', 'Belize', 'Honduras', 'Jamaica',
                                        'Trinidad and Tobago', 'Dominican Republic', 'Venezuela',
                                        'Haiti'], # Añadido
        "Southeastern Asia": ['Thailand', 'Singapore', 'Malaysia', 'Indonesia', 'Philippines', 'Cambodia',
                               'Vietnam', 'Myanmar', 'Laos'],
        "Central and Eastern Europe": ['Czech Republic', 'Slovakia', 'Slovenia', 'Poland', 'Hungary',
                                       'Croatia', 'Bosnia and Herzegovina', 'Serbia', 'Romania', 'Bulgaria',
                                       'Estonia', 'Latvia', 'Lithuania', 'Belarus', 'Moldova', 'Ukraine',
                                       'Kosovo', 'Macedonia', 'Montenegro', 'Russia', # Usamos 'Macedonia' si así está en el df_2017
                                       'Albania'], # Añadido
        "Eastern Asia": ['China', 'Hong Kong S.A.R., China', 'Japan', 'South Korea', 'Taiwan Province of China', 'Mongolia'],
        "Sub-Saharan Africa": ['Nigeria', 'South Africa', 'Kenya', 'Ethiopia', 'Uganda', 'Tanzania',
                               'Ghana', 'Senegal', 'Cameroon', 'Congo (Kinshasa)', 'Congo (Brazzaville)',
                               'Angola', 'Benin', 'Burkina Faso', 'Rwanda', 'Zimbabwe', 'Zambia', 'Mozambique',
                               'Namibia', 'Madagascar', 'Botswana', 'Malawi', 'Niger', 'Mali', 'Chad',
                               'Central African Republic', 'South Sudan', 'Somalia', 'Sierra Leone',
                               'Liberia', 'Guinea', 'Ivory Coast', 'Mauritius',
                               'Gabon', 'Mauritania', 'Lesotho', 'Togo', 'Burundi'], # Añadidos
        "Southern Asia": ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Afghanistan',
                           'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan']
    },
    2018: {
        "Western Europe": ['Finland', 'Denmark', 'Iceland', 'Switzerland', 'Netherlands', 'Austria',
                            'Ireland', 'Germany', 'Belgium', 'Luxembourg', 'United Kingdom', 'France',
                            'Spain', 'Portugal', 'Italy', 'Malta', 'Northern Cyprus', 'Cyprus', 'Norway', 'Sweden',
                            'Greece'], # Añadido
        "North America": ['Canada', 'United States'],
        "Australia and New Zealand": ['Australia', 'New Zealand'],
        "Middle East and Northern Africa": ['United Arab Emirates', 'Israel', 'Saudi Arabia', 'Qatar',
                                             'Bahrain', 'Kuwait', 'Jordan', 'Lebanon', 'Egypt', 'Libya',
                                             'Tunisia', 'Morocco', 'Algeria', 'Syria', 'Turkey', 'Iran', 'Iraq', 'Yemen',
                                             'Azerbaijan', 'Palestinian Territories', 'Georgia', 'Armenia'], # Añadidos
        "Latin America and Caribbean": ['Costa Rica', 'Chile', 'Argentina', 'Mexico', 'Guatemala', 'Panama',
                                        'Colombia', 'Brazil', 'Uruguay', 'Paraguay', 'Peru', 'Bolivia',
                                        'Ecuador', 'El Salvador', 'Nicaragua', 'Belize', 'Honduras', 'Jamaica',
                                        'Dominican Republic', 'Trinidad & Tobago', 'Venezuela',
                                        'Haiti'], # Añadido
        "Southeastern Asia": ['Thailand', 'Singapore', 'Malaysia', 'Indonesia', 'Philippines', 'Vietnam',
                               'Cambodia', 'Myanmar', 'Laos'],
        "Central and Eastern Europe": ['Czech Republic', 'Slovakia', 'Slovenia', 'Poland', 'Hungary',
                                       'Croatia', 'Bosnia and Herzegovina', 'Serbia', 'Romania', 'Bulgaria',
                                       'Estonia', 'Latvia', 'Lithuania', 'Belarus', 'Moldova', 'Kosovo',
                                       'Macedonia', # Asumiendo que 'Macedonia' es el nombre en el df
                                       'North Macedonia', # Incluir ambos si no estás seguro del nombre exacto en el df
                                       'Montenegro', 'Russia', 'Ukraine', 'Albania'],
        "Eastern Asia": ['China', 'Hong Kong', 'Japan', 'South Korea', 'Taiwan', 'Mongolia'],
        "Sub-Saharan Africa": ['Nigeria', 'South Africa', 'Kenya', 'Ethiopia', 'Uganda', 'Tanzania',
                               'Ghana', 'Senegal', 'Cameroon', 'Congo (Kinshasa)', 'Congo (Brazzaville)',
                               'Angola', 'Benin', 'Burkina Faso', 'Rwanda', 'Zimbabwe', 'Zambia', 'Mozambique',
                               'Namibia', 'Madagascar', 'Botswana', 'Malawi', 'Niger', 'Mali', 'Chad',
                               'Central African Republic', 'South Sudan', 'Somalia', 'Sierra Leone',
                               'Liberia', 'Guinea', 'Ivory Coast', 'Mauritius', 'Gabon', 'Sudan',
                               'Mauritania', 'Lesotho', 'Togo', 'Burundi'],
        "Southern Asia": ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Afghanistan',
                           'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan']
    },
    2019: {
        "Western Europe": ['Finland', 'Denmark', 'Norway', 'Iceland', 'Netherlands', 'Switzerland',
                            'Sweden', 'Austria', 'Luxembourg', 'United Kingdom', 'Ireland', 'Germany',
                            'Belgium', 'France', 'Spain', 'Portugal', 'Italy', 'Northern Cyprus', 'Cyprus', 'Malta', 'Greece'],
        "North America": ['Canada', 'United States'],
        "Australia and New Zealand": ['Australia', 'New Zealand'],
        "Middle East and Northern Africa": ['United Arab Emirates', 'Israel', 'Saudi Arabia', 'Qatar',
                                             'Bahrain', 'Kuwait', 'Jordan', 'Lebanon', 'Egypt', 'Libya',
                                             'Tunisia', 'Morocco', 'Algeria', 'Sudan', 'Palestinian Territories',
                                             'Syria', 'Armenia', 'Azerbaijan', 'Georgia', 'Iran', 'Iraq', 'Yemen', 'Turkey'],
        "Latin America and Caribbean": ['Chile', 'Guatemala', 'Costa Rica', 'Uruguay', 'Panama', 'Mexico',
                                        'Brazil', 'Argentina', 'El Salvador', 'Nicaragua', 'Colombia',
                                        'Ecuador', 'Peru', 'Bolivia', 'Paraguay', 'Venezuela',
                                        'Belize', 'Honduras', 'Jamaica', 'Dominican Republic', 'Trinidad & Tobago', 'Haiti'],
        "Southeastern Asia": ['Singapore', 'Thailand', 'Malaysia', 'Indonesia', 'Philippines', 'Vietnam',
                               'Cambodia', 'Myanmar', 'Laos'],
        "Central and Eastern Europe": ['Czech Republic', 'Slovakia', 'Slovenia', 'Poland', 'Hungary',
                                       'Croatia', 'Bosnia and Herzegovina', 'Serbia', 'Romania', 'Bulgaria',
                                       'Estonia', 'Latvia', 'Lithuania', 'Belarus', 'Moldova', 'Kosovo',
                                       'North Macedonia', 'Montenegro', 'Russia', 'Ukraine', 'Albania'],
        "Eastern Asia": ['China', 'Hong Kong', 'Japan', 'South Korea', 'Taiwan', 'Mongolia'],
        "Sub-Saharan Africa": ['Nigeria', 'South Africa', 'Kenya', 'Ethiopia', 'Uganda', 'Tanzania',
                               'Ghana', 'Senegal', 'Cameroon', 'Congo (Kinshasa)', 'Congo (Brazzaville)',
                               'Angola', 'Benin', 'Burkina Faso', 'Rwanda', 'Zimbabwe', 'Zambia', 'Mozambique',
                               'Namibia', 'Madagascar', 'Botswana', 'Malawi', 'Niger', 'Mali', 'Chad',
                               'Central African Republic', 'South Sudan', 'Somalia', 'Sierra Leone',
                               'Liberia', 'Guinea', 'Ivory Coast', 'Mauritius', 'Gabon',
                               'Mauritania', 'Lesotho', 'Togo', 'Burundi', 'Comoros', 'Swaziland',
                               'Gambia'], # Añadido Gambia
        "Southern Asia": ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Afghanistan',
                           'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan']
    }
}

# --- FUNCIONES AUXILIARES DE TRANSFORMACIÓN ---

def _rename_columns(df, year):
    """Renombra las columnas del DataFrame según el mapeo del año."""
    if year in COLUMN_MAPPINGS_BY_YEAR:
        df.rename(columns=COLUMN_MAPPINGS_BY_YEAR[year], inplace=True)
        logger.info(f"Columnas renombradas para el año {year}.")
    else:
        logger.warning(f"No se encontró mapeo de columnas para el año {year}.")
    return df

def _drop_columns(df, year):
    """Elimina columnas no deseadas del DataFrame según el año."""
    if year in COLUMNS_TO_DROP_BY_YEAR:
        cols_to_drop = [col for col in COLUMNS_TO_DROP_BY_YEAR[year] if col in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
            logger.info(f"Columnas eliminadas para el año {year}: {cols_to_drop}")
        else:
            logger.info(f"No hay columnas para eliminar o ya fueron eliminadas para el año {year}.")
    return df

def _add_year_column(df, year):
    """Añade la columna 'year'."""
    df['year'] = year
    logger.info(f"Columna 'year' ({year}) añadida.")
    return df

def _clean_string_columns(df):
    """Limpia espacios en columnas de tipo string (country, region)."""
    if 'country' in df.columns:
        df['country'] = df['country'].str.strip()
    if 'region' in df.columns: # Solo si ya existe
        df['region'] = df['region'].str.strip()
    logger.info("Valores de columnas string limpiados (strip).")
    return df

def _create_region_column(df, year):
    """Crea la columna 'region' para los años que no la tienen, usando el mapeo."""
    if year in COUNTRY_TO_REGION_MAPS:
        logger.info(f"Creando columna 'region' para el año {year} mediante mapeo.")
        region_lookup = {}
        for region, countries in COUNTRY_TO_REGION_MAPS[year].items():
            for country_name in countries:
                region_lookup[country_name.strip()] = region # Asegurar que las llaves del mapa estén limpias
        
        # Asegurar que los nombres de país en el DF coincidan con el mapa
        # (Podrías necesitar un mapeo de nombres de país más robusto aquí si hay variaciones)
        df['region'] = df['country'].str.strip().map(region_lookup)
        
        unmapped = df[df['region'].isnull()]['country'].unique()
        if len(unmapped) > 0:
            logger.warning(f"Año {year}: Países no mapeados a región: {list(unmapped)}. Tendrán NaN en 'region'.")
    return df

def _impute_corruption_2018(df):
    """Imputa NaNs en 'perceptions_of_corruption' para 2018 con la mediana regional."""
    col_to_impute = 'perceptions_of_corruption'
    if col_to_impute in df.columns and df[col_to_impute].isnull().any():
        logger.info(f"Imputando NaNs en '{col_to_impute}' para 2018 con mediana regional.")
        df[col_to_impute] = df.groupby('region')[col_to_impute].transform(lambda x: x.fillna(x.median()))
        # Fallback si alguna región entera tiene NaNs (poco probable para una sola fila)
        if df[col_to_impute].isnull().any():
            global_median = df[col_to_impute].median()
            df[col_to_impute].fillna(global_median, inplace=True)
            logger.info(f"NaNs restantes en '{col_to_impute}' imputados con mediana global: {global_median}")
    return df

def _reorder_columns(df):
    """Reordena las columnas al orden estándar final."""
    existing_cols = [col for col in DESIRED_FINAL_COLUMN_ORDER if col in df.columns]
    missing_cols = [col for col in DESIRED_FINAL_COLUMN_ORDER if col not in df.columns]
    if missing_cols:
        logger.warning(f"Al reordenar, las siguientes columnas deseadas no se encontraron y serán omitidas: {missing_cols}")
    
    # Columnas extra que no están en el orden deseado serán eliminadas por esta reindexación.
    # Si deseas mantenerlas, deben añadirse a DESIRED_FINAL_COLUMN_ORDER.
    df = df[existing_cols]
    logger.info("Columnas reordenadas al formato estándar.")
    return df

# --- FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN ---

def transform_single_dataframe(df_raw, year):
    """
    Aplica todas las transformaciones a un único DataFrame crudo para un año específico.
    """
    logger.info(f"--- Iniciando transformación para el año {year} ---")
    df_clean = df_raw.copy()

    # 1. Renombrar columnas
    df_clean = _rename_columns(df_clean, year)

    # 2. Eliminar columnas no deseadas (usa nombres originales)
    #    Esto debe hacerse ANTES de renombrar si los nombres en COLUMNS_TO_DROP_BY_YEAR son los originales.
    #    O DESPUÉS si COLUMNS_TO_DROP_BY_YEAR usa los nombres estándar.
    #    Vamos a asumir que COLUMNS_TO_DROP_BY_YEAR usa nombres originales (como en el notebook de limpieza)
    #    Así que esta función auxiliar debe ser llamada ANTES de _rename_columns o _rename_columns debe ser inteligente.
    #    Por simplicidad, y dado que el mapeo ya excluye las columnas a dropear,
    #    podríamos hacer el drop DESPUÉS del rename, usando los nombres que NO están en STANDARD_COLUMN_NAMES.
    #    O, mejor, tener COLUMNS_TO_DROP_BY_YEAR con los nombres originales y dropear antes.
    
    # Ajuste: Dropear columnas usando sus nombres originales ANTES de renombrar el resto.
    # Primero, aplicamos el drop con los nombres originales del CSV para ese año.
    df_clean = _drop_columns(df_clean.copy(), year) # Usar copia para el drop por si acaso

    # 3. Limpiar valores en 'country' (y 'region' si existe)
    df_clean = _clean_string_columns(df_clean)
    
    # 4. Crear columna 'region' si no existe (para 2017, 2018, 2019)
    if 'region' not in df_clean.columns and year in COUNTRY_TO_REGION_MAPS:
        df_clean = _create_region_column(df_clean, year)
    elif 'region' in df_clean.columns: # Asegurar limpieza si ya existe
        df_clean['region'] = df_clean['region'].str.strip()
        
    # 5. Imputar NaNs específicos del año (ej. 2018)
    if year == 2018:
        # La imputación por mediana regional necesita que la columna 'region' exista y esté poblada.
        if 'region' in df_clean.columns and not df_clean['region'].isnull().all():
            df_clean = _impute_corruption_2018(df_clean)
        else:
            logger.warning(f"Año {year}: No se pudo imputar '{'perceptions_of_corruption'}' por región porque la columna 'region' es inválida o no existe.")
            # Podrías añadir un fallback a la mediana global si es necesario.

    # 6. Añadir columna 'year'
    df_clean = _add_year_column(df_clean, year)
    
    # 7. Reordenar y seleccionar columnas finales
    df_clean = _reorder_columns(df_clean)
    
    # 8. Verificar tipos de datos (opcional aquí, se puede hacer en task.py)
    logger.info(f"Tipos de datos finales para el año {year}:\n{df_clean.dtypes}")
    
    logger.info(f"--- Transformación para el año {year} completada ---")
    return df_clean


def transform_all_dataframes(df_2015_raw, df_2016_raw, df_2017_raw, df_2018_raw, df_2019_raw):
    """
    Función principal llamada por task_transform_data.
    Transforma los 5 DataFrames crudos.
    Retorna una tupla de 5 DataFrames limpios.
    """
    logger.info("Iniciando transform_all_dataframes.")
    
    df_2015_cleaned = transform_single_dataframe(df_2015_raw, 2015)
    df_2016_cleaned = transform_single_dataframe(df_2016_raw, 2016)
    df_2017_cleaned = transform_single_dataframe(df_2017_raw, 2017)
    df_2018_cleaned = transform_single_dataframe(df_2018_raw, 2018)
    df_2019_cleaned = transform_single_dataframe(df_2019_raw, 2019)
    
    logger.info("Todos los DataFrames han sido transformados.")
    return df_2015_cleaned, df_2016_cleaned, df_2017_cleaned, df_2018_cleaned, df_2019_cleaned

# --- Bloque para pruebas si se ejecuta el script directamente ---
if __name__ == '__main__':
    logger.info("Ejecutando transform.py como script independiente para pruebas.")
    
    # Crear DataFrames dummy para probar la lógica de transformación
    # Deberías usar los CSVs reales para una prueba completa
    test_data_path = "/home/nicolas/Escritorio/workshops_ETL/workshop_3/data/raw/"
    try:
        df_2015_r = pd.read_csv(os.path.join(test_data_path, "2015.csv"))
        df_2016_r = pd.read_csv(os.path.join(test_data_path, "2016.csv"))
        df_2017_r = pd.read_csv(os.path.join(test_data_path, "2017.csv"))
        df_2018_r = pd.read_csv(os.path.join(test_data_path, "2018.csv"), na_values=['N/A'])
        df_2019_r = pd.read_csv(os.path.join(test_data_path, "2019.csv"))

        dfs_cleaned = transform_all_dataframes(df_2015_r, df_2016_r, df_2017_r, df_2018_r, df_2019_r)
        
        for i, df_c in enumerate(dfs_cleaned):
            year_test = 2015 + i
            if df_c is not None:
                logger.info(f"DataFrame Limpio para {year_test}: {df_c.shape}, Columnas: {df_c.columns.tolist()}")
                print(f"\n--- DataFrame Limpio {year_test} (Prueba) ---")
                print(df_c.head(2).to_markdown(index=False))
                df_c.info()
                # Verificar nulos en 'perceptions_of_corruption' para 2018
                if year_test == 2018:
                    logger.info(f"Nulos en 'perceptions_of_corruption' para 2018 después de imputar: {df_c['perceptions_of_corruption'].isnull().sum()}")

    except Exception as e:
        logger.error(f"Error durante la prueba del script de transformación: {e}", exc_info=True)