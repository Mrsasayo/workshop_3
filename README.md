```markdown
# Proyecto ETL y Predicción de Score de Felicidad Mundial

Este proyecto implementa un pipeline completo de ETL (Extracción, Transformación, Carga) y Machine Learning para analizar y predecir el score de felicidad mundial utilizando datasets de varios años. El pipeline está orquestado con Apache Airflow y utiliza Kafka para el streaming de datos en tiempo real para predicciones, con los resultados almacenados en PostgreSQL.

## Tabla de Contenidos
1.  [Visión General del Proyecto](#visión-general-del-proyecto)
2.  [Objetivos](#objetivos)
3.  [Tecnologías Utilizadas](#tecnologías-utilizadas)
4.  [Estructura del Proyecto](#estructura-del-proyecto)
5.  [Flujo de Datos y Procesos](#flujo-de-datos-y-procesos)
    *   [Pipeline de Airflow](#pipeline-de-airflow)
    *   [Consumidor de Kafka y Almacenamiento de Predicciones](#consumidor-de-kafka-y-almacenamiento-de-predicciones)
6.  [Configuración y Ejecución](#configuración-y-ejecución)
    *   [Prerrequisitos](#prerrequisitos)
    *   [Configuración del Entorno](#configuración-del-entorno)
    *   [Iniciar Servicios (Kafka, Airflow)](#iniciar-servicios-kafka-airflow)
    *   [Ejecutar el Pipeline](#ejecutar-el-pipeline)
    *   [Ejecutar el Consumidor de Predicciones](#ejecutar-el-consumidor-de-predicciones)
7.  [Análisis y Resultados](#análisis-y-resultados)
8.  [Posibles Mejoras Futuras](#posibles-mejoras-futuras)

## Visión General del Proyecto

El proyecto toma datos crudos del score de felicidad mundial de los años 2015 a 2019. Estos datasets se limpian, transforman y unifican. Posteriormente, se entrena un modelo de Machine Learning (Gradient Boosting Regressor) para predecir el score de felicidad. Una porción de los datos se envía a través de un topic de Kafka para simular un flujo de datos para predicción en tiempo real. Un consumidor de Kafka lee estos datos, utiliza el modelo entrenado para hacer predicciones y almacena los resultados en una base de datos PostgreSQL.

## Objetivos

*   Implementar un pipeline ETL robusto para la ingesta y procesamiento de datos.
*   Limpiar y unificar datasets de múltiples fuentes/años.
*   Entrenar un modelo de regresión para predecir el score de felicidad.
*   Utilizar Apache Kafka para el streaming de datos para predicciones.
*   Orquestar el flujo de trabajo completo con Apache Airflow.
*   Almacenar y analizar los resultados de las predicciones.

## Tecnologías Utilizadas

*   **Python 3.10+**
*   **Pandas:** Para manipulación y análisis de datos.
*   **NumPy:** Para operaciones numéricas.
*   **Scikit-learn:** Para preprocesamiento de datos y modelado de Machine Learning (GradientBoostingRegressor, StandardScaler, OneHotEncoder, Pipeline, ColumnTransformer).
*   **Joblib:** Para guardar y cargar modelos entrenados.
*   **Apache Airflow (v2.10.0):** Para la orquestación del pipeline.
*   **Apache Kafka:** Para el sistema de mensajería y streaming de datos.
    *   Se utiliza con Docker a través de `docker-compose.yml` (Confluent Kafka & Zookeeper).
*   **PostgreSQL:** Como base de datos para almacenar las predicciones.
*   **Psycopg2:** Adaptador de Python para PostgreSQL.
*   **python-dotenv:** Para la gestión de variables de entorno.
*   **Jupyter Notebooks:** Para análisis exploratorio de datos (EDA) y visualización de resultados.
*   **Matplotlib & Seaborn:** Para la visualización de datos.
*   **Statsmodels:** Para gráficos específicos como el Mosaic Plot.

## Estructura del Proyecto

```
.
├── airflow/
│ ├── dags/
│ │ ├── dag.py
│ │ └── task.py
│ ├── airflow.cfg
│ ├── airflow.db
│ └── logs/
├── config/
│ ├── .env
│ └── airflow_setup.txt
├── data/
│ ├── raw/
│ │ ├── 2015.csv
│ │ └── ... (otros archivos CSV de años)
│ ├── processed/
│ │ ├── 2015_cleaned.csv
│ │ └── happiness_unified_dataset.csv
│ ├── predictions_output.csv
│ └── 001_mini_eda_predicciones.ipynb
├── docker-compose.yml
├── etl/
│ ├── extract/
│ │ └── extract.py
│ ├── load/
│ │ └── save_transformed_datasets.py
│ │ └── save_unified_dataset.py
│ ├── merge/
│ │ └── merge.py
│ └── transform/
│ └── transform.py
├── ml/
│ ├── predict/
│ │ └── predict_consumer.py
│ └── train/
│ └── train.py
├── models/
│ └── trained_happiness_model_pipeline.joblib
├── notebooks/
│ ├── 001_merge.ipynb
│ └── ... (otros notebooks de EDA y limpieza)
├── streaming/
│ └── producer/
│ └── producer.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Flujo de Datos y Procesos

### Pipeline de Airflow (`happiness_etl_ml_pipeline`)

El DAG de Airflow orquesta las siguientes tareas secuencialmente y en paralelo donde es posible:

1.  **`1_extraccion`**: Carga los 5 datasets CSV crudos (2015-2019) desde `data/raw/`.
2.  **`2_transformacion`**:
    *   Recibe los 5 DataFrames crudos.
    *   Limpia los datos: renombra columnas, maneja valores faltantes (ej. imputación para `perceptions_of_corruption` en 2018), crea la columna `region` donde sea necesario, y añade la columna `year`.
    *   Estandariza el formato de las columnas.
3.  **`3_2_carga_individual_limpios`** (paralelo a 3.1): Guarda cada uno de los 5 DataFrames limpios como archivos CSV en `data/processed/` para verificación.
4.  **`3_1_merge_split_datos`**:
    *   Recibe los 5 DataFrames limpios.
    *   Los concatena en un único DataFrame (`df_unified_full`).
    *   Divide `df_unified_full` en:
        *   `df_train` (80% de los datos) para el entrenamiento del modelo.
        *   `df_predict_stream` (20% de los datos) para ser enviados a Kafka.
5.  **`4_1_entrenamiento_modelo`**:
    *   Utiliza `df_train`.
    *   Preprocesa los datos (StandardScaler para numéricas, OneHotEncoder para 'region').
    *   Entrena un modelo `GradientBoostingRegressor`.
    *   Guarda el pipeline de preprocesamiento y modelo entrenado en `models/trained_happiness_model_pipeline.joblib`.
6.  **`4_2_kafka_producer`**:
    *   Toma `df_predict_stream`.
    *   Envía cada fila como un mensaje JSON al topic de Kafka `happiness_data_to_predict`.
7.  **`4_3_carga_merge_completo`**: Guarda el `df_unified_full` (100% de los datos unificados) como un archivo CSV en `data/processed/happiness_unified_dataset.csv`.

### Consumidor de Kafka y Almacenamiento de Predicciones (`ml/predict/predict_consumer.py`)

Este es un script independiente que se ejecuta fuera del pipeline de Airflow:

1.  **Carga el Modelo:** Carga el `trained_happiness_model_pipeline.joblib` desde la carpeta `models/`.
2.  **Consume de Kafka:** Se suscribe al topic `happiness_data_to_predict`.
3.  **Realiza Predicciones:** Para cada mensaje recibido (que representa un registro de datos):
    *   Realiza la predicción del score de felicidad usando el modelo cargado.
4.  **Verifica Duplicados:** Antes de insertar en PostgreSQL, verifica si ya existe una predicción para las mismas features (excluyendo el score) para evitar duplicados.
5.  **Almacena en PostgreSQL:** Inserta el registro original junto con el `predicted_happiness_score` en la tabla `happiness_predictions` de la base de datos PostgreSQL.
6.  **Guarda en CSV:** Acumula todas las predicciones procesadas y las guarda en `data/predictions_output.csv` al finalizar su ejecución (o periódicamente).
7.  **Tiempo de Ejecución:** El consumidor está configurado para ejecutarse por un tiempo limitado (ej. 5 minutos) para este proyecto.

## Configuración y Ejecución

### Prerrequisitos

*   Python 3.10 o superior.
*   `pip` y `venv`.
*   Docker y Docker Compose.
*   Un servidor PostgreSQL accesible (puede ser local o en Docker).

### Configuración del Entorno

1.  **Clonar el repositorio (si aplica).**
2.  **Navegar al directorio raíz del proyecto.**
3.  **Crear y activar un entorno virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate 
    ```
4.  **Instalar dependencias:**
    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```
5.  **Configurar variables de entorno:**
    *   Copia `config/.env.example` a `config/.env` (si tienes un archivo de ejemplo).
    *   Edita `config/.env` con tus credenciales de PostgreSQL:
        ```env
        POSTGRES_USER=tu_usuario_postgres
        POSTGRES_PASSWORD=tu_contraseña_postgres
        POSTGRES_HOST=localhost # o la IP/hostname de tu servidor PG
        POSTGRES_PORT=5432
        POSTGRES_DATABASE=mrsasayo # o el nombre de tu BD
        # Opcional para el consumidor de Kafka
        # KAFKA_BOOTSTRAP_SERVERS=localhost:29092
        # KAFKA_PREDICT_TOPIC=happiness_data_to_predict
        # MODEL_FILENAME=trained_happiness_model_pipeline.joblib
        # PG_PREDICTIONS_TABLE=happiness_predictions
        ```

### Iniciar Servicios (Kafka, Airflow)

1.  **Iniciar Kafka y Zookeeper:**
    Desde el directorio raíz del proyecto (donde está `docker-compose.yml`):
    ```bash
    docker-compose up -d
    ```
    Para detenerlos: `docker-compose down`

2.  **Configurar e Iniciar Airflow (Standalone para desarrollo):**
    Sigue las instrucciones en `config/airflow_setup.txt`. Los pasos clave son:
    ```bash
    export AIRFLOW_HOME="$(pwd)/airflow"
    # (Configurar AIRFLOW_VERSION y CONSTRAINT_URL como en el archivo)
    # pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    # pip install "apache-airflow-providers-postgres" ...
    airflow db init
    airflow users create --username admin --firstname TuNombre --lastname TuApellido --role Admin --email tu@email.com # (establece tu contraseña)
    airflow standalone
    ```
    La UI de Airflow estará disponible en `http://localhost:8080`.

### Ejecutar el Pipeline

1.  Asegúrate de que los servicios de Kafka y Airflow (standalone) estén corriendo.
2.  Abre la UI de Airflow (`http://localhost:8080`).
3.  Busca el DAG `happiness_etl_ml_pipeline`.
4.  Desactiva la pausa (toggle) del DAG si está pausado.
5.  Dispara el DAG manualmente usando el botón "Play".

### Ejecutar el Consumidor de Predicciones

Después de que la tarea `4_2_kafka_producer` del DAG de Airflow haya completado y enviado datos a Kafka:

1.  Abre una nueva terminal.
2.  Activa el entorno virtual: `source venv/bin/activate`
3.  Navega al directorio raíz del proyecto.
4.  Ejecuta el script del consumidor:
    ```bash
    python ml/predict/predict_consumer.py
    ```
    El consumidor se ejecutará por el tiempo definido en `CONSUMER_RUN_DURATION_MINUTES` dentro del script, procesando mensajes de Kafka y guardando predicciones.

## Análisis y Resultados

El notebook `data/001_mini_eda_predicciones.ipynb` se utiliza para:
1.  Cargar las predicciones almacenadas en PostgreSQL (o desde `predictions_output.csv`).
2.  Comparar los scores de felicidad reales con los predichos.
3.  Visualizar el rendimiento del modelo mediante:
    *   Gráficos de dispersión (Real vs. Predicho, Residuos vs. Predicho/Real).
    *   Histogramas de residuos y de scores.
    *   Box plots de errores por región.
    *   Gráficos de líneas de scores promedio por año.
    *   Análisis de los errores más grandes para identificar patrones o casos problemáticos.
4.  Calcular métricas de error como MAE, MSE, RMSE y R².

Las conclusiones de este análisis ayudan a entender la precisión del modelo, sus sesgos y áreas de posible mejora.

## Posibles Mejoras Futuras

*   **Optimización de Hiperparámetros:** Realizar una búsqueda más exhaustiva de hiperparámetros para el `GradientBoostingRegressor`.
*   **Ingeniería de Features Avanzada:** Explorar transformaciones no lineales, términos de interacción, o la creación de nuevas features a partir de las existentes.
*   **Manejo de Datos de 2015:** Investigar más a fondo por qué los datos de 2015 generan una mayor proporción de errores grandes.
*   **Modelos Específicos por Región:** Considerar entrenar modelos separados para regiones con características muy dispares si el rendimiento global no mejora suficientemente.
*   **Modelos Alternativos:** Probar otros algoritmos de regresión (ej. XGBoost, LightGBM, Redes Neuronales).
*   **Pipeline de Airflow en Docker:** Migrar la configuración de Airflow a Docker para un entorno más aislado y portable.
*   **Monitoreo Continuo:** Implementar un sistema para monitorear el rendimiento del modelo a medida que llegan nuevos datos (si fuera un sistema en producción).
*   **Mejorar la Robustez del Consumidor:** Añadir manejo de reintentos más sofisticado, dead-letter queues para mensajes fallidos en Kafka.

```

**Puntos a Considerar para tu README:**

*   **Personalización:** Reemplaza los placeholders como `tu_usuario_postgres`, `tu_contraseña_postgres`, `tu@email.com`, etc., con información genérica o instrucciones claras para el usuario.
*   **`.env.example`:** Es una buena práctica incluir un archivo `config/.env.example` en tu repositorio con la estructura de las variables de entorno necesarias, pero sin los valores sensibles. El usuario lo copiaría a `.env` y lo rellenaría.
*   **Versiones:** Has especificado Airflow v2.10.0. Si usas versiones específicas para otras librerías clave (como Kafka, PostgreSQL si lo dockerizas), menciónalas.
*   **Detalles de Instalación:** La referencia a `config/airflow_setup.txt` es buena. Asegúrate de que ese archivo esté actualizado y sea claro.
*   **Claridad en la Ejecución:** Sé lo más explícito posible en los pasos para configurar y ejecutar el proyecto.