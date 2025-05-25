# /home/nicolas/Escritorio/workshops_ETL/workshop_3/etl/transform/transform.py

import pandas as pd
import numpy as np 
import logging
import os
import sys

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


STANDARD_COLUMN_NAMES = {
    'country': 'country',
    'region': 'region',
    'happiness_rank': 'happiness_rank',
    'happiness_score': 'happiness_score',
    'economy_gdp_per_capita': 'economy_gdp_per_capita',
    'social_support': 'social_support', 
    'health_life_expectancy': 'health_life_expectancy',
    'freedom_to_make_life_choices': 'freedom_to_make_life_choices',
    'perceptions_of_corruption': 'perceptions_of_corruption',
    'generosity': 'generosity',
    'year': 'year'
}

DESIRED_FINAL_COLUMN_ORDER = [
    'year', 'region', 'country', 'happiness_rank', 'happiness_score',
    'social_support', 'health_life_expectancy', 'generosity',
    'freedom_to_make_life_choices', 'economy_gdp_per_capita',
    'perceptions_of_corruption'
]

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

COLUMNS_TO_DROP_BY_YEAR = {
    2015: ['Standard Error', 'Dystopia Residual'],
    2016: ['Lower Confidence Interval', 'Upper Confidence Interval', 'Dystopia Residual'],
    2017: ['Whisker.high', 'Whisker.low', 'Dystopia.Residual'],
    2018: [],
    2019: []
}

COUNTRY_TO_REGION_MAPS = {
    2017: {
        "Western Europe": ['Norway', 'Denmark', 'Iceland', 'Switzerland', 'Finland', 'Netherlands',
                            'Austria', 'Ireland', 'Germany', 'Belgium', 'Luxembourg', 'United Kingdom',
                            'France', 'Spain', 'Italy', 'Portugal', 'Cyprus', 'Malta', 'Greece', 'North Cyprus',
                            'Sweden'],
        "North America": ['Canada', 'United States'],
        "Australia and New Zealand": ['Australia', 'New Zealand'],
        "Middle East and Northern Africa": ['United Arab Emirates', 'Israel', 'Saudi Arabia', 'Qatar',
                                             'Bahrain', 'Kuwait', 'Jordan', 'Lebanon', 'Egypt', 'Libya',
                                             'Tunisia', 'Morocco', 'Algeria', 'Sudan', 'Palestinian Territories',
                                             'Syria', 'Armenia', 'Azerbaijan', 'Georgia',
                                             'Turkey', 'Iran', 'Iraq', 'Yemen'],
        "Latin America and Caribbean": ['Costa Rica', 'Chile', 'Argentina', 'Mexico', 'Guatemala', 'Panama',
                                        'Colombia', 'Brazil', 'Uruguay', 'Paraguay', 'Peru', 'Bolivia',
                                        'Ecuador', 'El Salvador', 'Nicaragua', 'Belize', 'Honduras', 'Jamaica',
                                        'Trinidad and Tobago', 'Dominican Republic', 'Venezuela',
                                        'Haiti'],
        "Southeastern Asia": ['Thailand', 'Singapore', 'Malaysia', 'Indonesia', 'Philippines', 'Cambodia',
                               'Vietnam', 'Myanmar', 'Laos'],
        "Central and Eastern Europe": ['Czech Republic', 'Slovakia', 'Slovenia', 'Poland', 'Hungary',
                                       'Croatia', 'Bosnia and Herzegovina', 'Serbia', 'Romania', 'Bulgaria',
                                       'Estonia', 'Latvia', 'Lithuania', 'Belarus', 'Moldova', 'Ukraine',
                                       'Kosovo', 'Macedonia', 'Montenegro', 'Russia',
                                       'Albania'],
        "Eastern Asia": ['China', 'Hong Kong S.A.R., China', 'Japan', 'South Korea', 'Taiwan Province of China', 'Mongolia'],
        "Sub-Saharan Africa": ['Nigeria', 'South Africa', 'Kenya', 'Ethiopia', 'Uganda', 'Tanzania',
                               'Ghana', 'Senegal', 'Cameroon', 'Congo (Kinshasa)', 'Congo (Brazzaville)',
                               'Angola', 'Benin', 'Burkina Faso', 'Rwanda', 'Zimbabwe', 'Zambia', 'Mozambique',
                               'Namibia', 'Madagascar', 'Botswana', 'Malawi', 'Niger', 'Mali', 'Chad',
                               'Central African Republic', 'South Sudan', 'Somalia', 'Sierra Leone',
                               'Liberia', 'Guinea', 'Ivory Coast', 'Mauritius',
                               'Gabon', 'Mauritania', 'Lesotho', 'Togo', 'Burundi'],
        "Southern Asia": ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Afghanistan',
                           'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan']
    },
    2018: {
        "Western Europe": ['Finland', 'Denmark', 'Iceland', 'Switzerland', 'Netherlands', 'Austria',
                            'Ireland', 'Germany', 'Belgium', 'Luxembourg', 'United Kingdom', 'France',
                            'Spain', 'Portugal', 'Italy', 'Malta', 'Northern Cyprus', 'Cyprus', 'Norway', 'Sweden',
                            'Greece'],
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
                                        'Haiti'],
        "Southeastern Asia": ['Thailand', 'Singapore', 'Malaysia', 'Indonesia', 'Philippines', 'Vietnam',
                               'Cambodia', 'Myanmar', 'Laos'],
        "Central and Eastern Europe": ['Czech Republic', 'Slovakia', 'Slovenia', 'Poland', 'Hungary',
                                       'Croatia', 'Bosnia and Herzegovina', 'Serbia', 'Romania', 'Bulgaria',
                                       'Estonia', 'Latvia', 'Lithuania', 'Belarus', 'Moldova', 'Kosovo',
                                       'Macedonia',
                                       'North Macedonia', 
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
                               'Gambia'],
        "Southern Asia": ['India', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Afghanistan',
                           'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan']
    }
}


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
    if 'region' in df.columns: 
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
                region_lookup[country_name.strip()] = region

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
    
    df = df[existing_cols]
    logger.info("Columnas reordenadas al formato estándar.")
    return df


def transform_single_dataframe(df_raw, year):
    """
    Aplica todas las transformaciones a un único DataFrame crudo para un año específico.
    """
    logger.info(f"--- Iniciando transformación para el año {year} ---")
    df_clean = df_raw.copy()

    df_clean = _rename_columns(df_clean, year)

    df_clean = _drop_columns(df_clean.copy(), year) 
    df_clean = _clean_string_columns(df_clean)
    
    if 'region' not in df_clean.columns and year in COUNTRY_TO_REGION_MAPS:
        df_clean = _create_region_column(df_clean, year)
    elif 'region' in df_clean.columns:
        df_clean['region'] = df_clean['region'].str.strip()
        
    if year == 2018:
        if 'region' in df_clean.columns and not df_clean['region'].isnull().all():
            df_clean = _impute_corruption_2018(df_clean)
        else:
            logger.warning(f"Año {year}: No se pudo imputar '{'perceptions_of_corruption'}' por región porque la columna 'region' es inválida o no existe.")

    df_clean = _add_year_column(df_clean, year)
    
    df_clean = _reorder_columns(df_clean)
    
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

if __name__ == '__main__':
    logger.info("Ejecutando transform.py como script independiente para pruebas.")
    
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
                if year_test == 2018:
                    logger.info(f"Nulos en 'perceptions_of_corruption' para 2018 después de imputar: {df_c['perceptions_of_corruption'].isnull().sum()}")

    except Exception as e:
        logger.error(f"Error durante la prueba del script de transformación: {e}", exc_info=True)