# Entrega de Actividad: Pipeline ETL Orquestado con Apache Airflow

Este proyecto consiste en el desarrollo de un pipeline de datos (ETL) automatizado utilizando **Apache Airflow**. El objetivo principal fue extraer datos de una API pública, transformar la información bajo reglas de negocio específicas y cargar el resultado final en una base de datos relacional PostgreSQL.

##  Tecnologías Utilizadas

*   **Orquestador:** Apache Airflow (TaskFlow API)
*   **Lenguaje:** Python 3.x
*   **Base de Datos:** PostgreSQL
*   **Infraestructura:** Docker & Docker Compose
*   **Librerías Python:** `requests`, `airflow-providers-postgres`

##  Arquitectura del Pipeline (DAG)

El flujo de trabajo se definió en el DAG `tarea_etl_posts`, el cual consta de tres etapas principales conectadas de forma secuencial:

1.  **Extract (`extract_posts`):** Consumo de la API REST de [JSONPlaceholder](https://jsonplaceholder.typicode.com/posts) para obtener un listado de publicaciones en formato JSON. Se implementó validación de estados HTTP para asegurar la integridad de la extracción.
2.  **Transform (`transform_posts`):** Aplicación de lógica de filtrado sobre los datos crudos. Solo se conservaron los posts cuyo título contiene más de 5 palabras, calculando y agregando un nuevo campo `word_count` a cada registro.
3.  **Load (`load_to_postgres`):** Persistencia de los datos transformados en PostgreSQL. Se utilizó `PostgresHook` para manejar la conexión, creación de tablas dinámicas y la inserción eficiente de registros tras una limpieza previa de la tabla (Truncate).

## Ejecución del Proyecto

Para levantar el entorno y ejecutar el pipeline, se deben seguir estos pasos:

1.  Asegurarse de tener Docker y Docker Compose instalados.
2.  Levantar los servicios desde la raíz del proyecto:
    ```bash
    docker-compose up -d
    ```
3.  Acceder a la interfaz de Airflow en [http://localhost:8080](http://localhost:8080) (Credenciales: `airflow` / `airflow`).
4.  Activar y ejecutar el DAG `tarea_etl_posts`.

## Resultados Obtenidos

*   Automatización completa del proceso de ingesta y transformación.
*   Manejo eficiente de conexiones a base de datos mediante Hooks.
*   Estructura de código limpia y modular utilizando decoradores de Airflow (`@dag`, `@task`).
*   Persistencia exitosa en la tabla `posts_filtered` con los datos validados.
