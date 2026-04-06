# =============================================================================
# TAREA - ETL Pipeline Orquestado con Airflow
# =============================================================================
#
# Objetivo: Construir un pipeline ETL completo que:
#   1. Extraiga posts desde una API pública (JSONPlaceholder)
#   2. Aplique una transformación filtrando los posts relevantes
#   3. Cargue los resultados en la base de datos Postgres
#
# Referencia:
#   - Para el TaskFlow API (@dag, @task): revisar 04_ejercicio_01.py
#   - Para el PostgresHook: revisar 04_ejercicio_01.py -> save_users_extended()
#   - Para pasar datos entre tareas: revisar 03_ejercicio.py (XComs)
#
# Entorno:
#   - Conexión Postgres: 'postgres_datapath'
#   - API a consumir: https://jsonplaceholder.typicode.com/posts
#
# Instrucciones:
#   1. Busca todos los comentarios marcados con "# TODO"
#   2. Completa el código en cada sección
#   3. Copia este archivo dentro de la carpeta dags/ para que Airflow lo detecte
#   4. Activa el DAG desde la UI de Airflow (http://localhost:8080)
#   5. Verifica que las 3 tareas se ejecuten en verde
# =============================================================================

from airflow.decorators import dag, task # Para definir el flujo y las tareas
from airflow.providers.postgres.hooks.postgres import PostgresHook # Para hablar con la base de datos
from airflow.utils.dates import days_ago # Para manejar fechas de inicio
from datetime import timedelta # Para definir tiempos de espera
import requests # Para conectarnos a la API de internet

default_args = {
    'owner': 'Frank', # Quién es el responsable
    'depends_on_past': False, # Si una ejecución falló ayer, ¿debe correr hoy?
    'start_date': days_ago(1), # Empezar desde hace un día
    'retries': 1, # Si falla, intenta una vez más
    'retry_delay': timedelta(minutes=5), # Espera 5 minutos antes de reintentar
}

@dag(
    dag_id='tarea_etl_posts', # El nombre que verás en la web de Airflow
    default_args=default_args, 
    schedule_interval='@daily', # Se ejecuta una vez al día
    catchup=False, # No intentes ejecutar días pasados
    tags=['tarea', 'etl'] # Etiquetas para organizar en la UI
)


def etl_posts_flow():
    
    @task()
    def extract_posts():
        """Trae los posts desde la API pública"""
        url = "https://jsonplaceholder.typicode.com/posts"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    @task()
    def transform_posts(posts: list):
        """
        Filtra posts con títulos de más de 5 palabras 
        y añade el campo word_count.
        """
        if not posts:
            return []

        transformed = []

        for post in posts:
            title = post.get('title', '')
            word_count = len(title.split())
            if word_count > 5:
                transformed.append({
                    'id': post.get('id'),
                    'user_id': post.get('userId'),
                    'title': title,
                    'body': post.get('body'),
                    'word_count': word_count,
                })

        print(f"Filtrado completado: {len(transformed)} posts de {len(posts)} recibidos.")
        return transformed
    
    @task()
    def load_to_postgres(posts: list):
        if not posts:
            print("No hay datos para cargar.")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_datapath')

       
        create_sql = """
        CREATE TABLE IF NOT EXISTS posts_filtered (
            id         INT PRIMARY KEY,
            user_id    INT,
            title      TEXT,
            body       TEXT,
            word_count INT
        );
        """
        pg_hook.run(create_sql)

        
        pg_hook.run("TRUNCATE TABLE posts_filtered")

        
        rows_to_insert = []
        for post in posts:
            row = (
                post['id'],
                post['user_id'],
                post['title'],
                post['body'],
                post['word_count'],
            )
            rows_to_insert.append(row)

        
        pg_hook.insert_rows(
            table='posts_filtered',
            rows=rows_to_insert,
            target_fields=['id', 'user_id', 'title', 'body', 'word_count']
        )
        print(f"Éxito: Se cargaron {len(rows_to_insert)} filas en Postgres.")

        

    # --- AQUÍ VA EL FLUJO DEL PIPELINE ---
    # Es el "pegamento" que une las tareas
    
    # 1. Ejecutas la extracción y guardas el resultado en una variable
    posts_extraidos = extract_posts()
    
    # 2. Pasas ese resultado a la transformación
    posts_transformados = transform_posts(posts_extraidos)
    
    # 3. Finalmente, pasas los datos limpios a la carga
    load_to_postgres(posts_transformados)
    




# Esta línea es vital: crea la instancia del DAG para que Airflow la lea
dag_instance = etl_posts_flow()