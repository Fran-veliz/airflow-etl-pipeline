# Tarea - ETL Pipeline Orquestado con Airflow

## Objetivo

Construir un pipeline ETL completo en Airflow que consuma datos de una API pública, aplique una transformación y guarde los resultados en Postgres.

El ejercicio integra todo lo visto en el taller:
- Estructura de un DAG con el **TaskFlow API** (`@dag`, `@task`)
- Consumo de una **API REST** con `requests`
- **Transformación** de datos con Python
- **Carga a Postgres** con `PostgresHook`

---

## Entorno necesario

Antes de empezar, asegúrate de tener el entorno del taller corriendo.

Desde la raíz del proyecto (donde está el `docker-compose.yaml`), ejecuta:

```bash
docker-compose up -d
```

Verifica que todo esté levantado en [http://localhost:8080](http://localhost:8080)
- Usuario: `airflow`
- Contraseña: `airflow`

> Si es la primera vez que lo levantas, puede tardar unos minutos mientras se inicializa.

---

## Estructura del pipeline

El DAG `tarea_etl_posts` tiene 3 tareas conectadas en secuencia:

```
extract_posts  →  transform_posts  →  load_to_postgres
```

| Tarea | Qué hace |
|---|---|
| `extract_posts` | Llama a la API y trae todos los posts |
| `transform_posts` | Filtra posts cuyo título tenga más de 5 palabras y agrega el campo `word_count` |
| `load_to_postgres` | Crea la tabla en Postgres (si no existe) e inserta los posts filtrados |

---

## Paso a paso

### 1. Abre el archivo de la tarea

El archivo a completar es `tarea/tarea_etl_dag.py`. Ábrelo en tu editor.

Busca todos los comentarios marcados con `# TODO` — ahí es donde debes escribir el código.

---

### 2. Completa `extract_posts`

Esta tarea debe consumir el siguiente endpoint:

```
https://jsonplaceholder.typicode.com/posts
```

La respuesta es una lista de posts con esta estructura:

```json
{
  "userId": 1,
  "id": 1,
  "title": "sunt aut facere repellat provident occaecati",
  "body": "quia et suscipit..."
}
```

**Lo que debes hacer:**
1. Hacer el GET request a la URL con `requests.get(url)`
2. Verificar que no hubo error con `response.raise_for_status()`
3. Retornar la lista con `response.json()`

Referencia: mira `fetch_users_data()` en `dags/04_ejercicio_01.py`

---

### 3. Completa `transform_posts`

Esta tarea recibe la lista de posts y aplica las siguientes reglas:

- Calcular cuántas palabras tiene el título con `.split()`
- Conservar **solo** los posts cuyo título tenga **más de 5 palabras**
- Agregar el campo `word_count` a cada post

El diccionario que debes construir para cada post es:

```python
{
    'id': post.get('id'),
    'user_id': post.get('userId'),
    'title': title,
    'body': post.get('body'),
    'word_count': word_count,
}
```

Referencia: mira `apply_business_logic()` en `dags/04_ejercicio_02.py`

---

### 4. Completa `load_to_postgres`

Esta tarea guarda los posts en Postgres. Los pasos a seguir dentro de la función son:

**4.1 Crear el hook:**
```python
pg_hook = PostgresHook(postgres_conn_id='postgres_datapath')
```

**4.2 Crear la tabla si no existe:**
```sql
CREATE TABLE IF NOT EXISTS posts_filtered (
    id         INT PRIMARY KEY,
    user_id    INT,
    title      TEXT,
    body       TEXT,
    word_count INT
);
```

**4.3 Limpiar la tabla antes de insertar:**
```python
pg_hook.run("TRUNCATE TABLE posts_filtered")
```

**4.4 Preparar e insertar las filas:**
```python
pg_hook.insert_rows(
    table='posts_filtered',
    rows=rows_to_insert,
    target_fields=['id', 'user_id', 'title', 'body', 'word_count']
)
```

Referencia: mira `save_users_extended()` en `dags/04_ejercicio_01.py`

---

### 5. Conecta el flujo

Al final del DAG, en la sección **FLUJO DEL PIPELINE**, conecta las tareas pasando los resultados de una a la siguiente:

```python
raw_posts = extract_posts()
filtered_posts = transform_posts(raw_posts)
load_to_postgres(filtered_posts)
```

Referencia: mira cómo se conecta el flujo al final de `dags/04_ejercicio_01.py`

---

### 6. Despliega el DAG en Airflow

Copia el archivo terminado a la carpeta `dags/`:

```bash
cp tarea/tarea_etl_dag.py dags/tarea_etl_dag.py
```

Airflow detecta los cambios automáticamente. En unos segundos debería aparecer el DAG `tarea_etl_posts` en la UI.

---

### 7. Ejecuta y verifica

1. En la UI de Airflow, busca el DAG `tarea_etl_posts`
2. Actívalo con el toggle de la izquierda
3. Dispara una ejecución manual con el botón **Trigger DAG**
4. Verifica que las 3 tareas queden en **verde**

Si alguna tarea falla, revisa los logs haciendo clic sobre la tarea → **Logs**.

---

## Resultado esperado

Al terminar, deberías tener en Postgres una tabla `posts_filtered` con los posts cuyo título tiene más de 5 palabras, más el campo `word_count` calculado.

Puedes verificarlo conectándote a Postgres desde la UI de Airflow:
- Ve a **Admin → Connections** y busca `postgres_datapath`

O bien consultando directamente desde un cliente SQL con las credenciales del `docker-compose.yaml`.

---

## Checklist de entrega

- [ ] Las 3 tareas del DAG están completas (sin `pass` ni `# TODO` pendientes)
- [ ] El flujo al final del DAG conecta las 3 tareas en orden
- [ ] El DAG se ejecuta sin errores en Airflow
- [ ] La tabla `posts_filtered` existe en Postgres con datos cargados
