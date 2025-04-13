from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import requests
import snowflake.connector

TOTAL_PAGES = 5  # or however many pages you want to fetch

def fetch_popular_movies(**context):
    api_key = os.getenv("TMDB_API_KEY")
    base_url = "https://api.themoviedb.org/3/movie/popular"
    all_movies = []

    for page in range(1, TOTAL_PAGES + 1):
        url = f"{base_url}?api_key={api_key}&page={page}"
        response = requests.get(url)
        if response.status_code == 200:
            all_movies.extend(response.json().get("results", []))
        else:
            raise Exception(f"Failed on page {page}: {response.status_code}")

    context['ti'].xcom_push(key='movies', value=all_movies)
    print(f"✅ Fetched {len(all_movies)} movies.")

def load_movies_to_snowflake(**context):
    movies = context['ti'].xcom_pull(key='movies', task_ids='fetch_popular_movies')
    now = datetime.utcnow()  # current timestamp to label this ingestion batch

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    # Create table if not exists (with ingestion timestamp)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS popular_movies (
            id INTEGER,
            title STRING,
            release_date DATE,
            popularity FLOAT,
            vote_average FLOAT,
            vote_count INTEGER,
            ingested_at TIMESTAMP
        );
    """)

    for movie in movies:
        cursor.execute("""
            INSERT INTO popular_movies (
                id, title, release_date, popularity, vote_average, vote_count, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            movie["id"],
            movie["title"],
            movie.get("release_date"),
            movie.get("popularity"),
            movie.get("vote_average"),
            movie.get("vote_count"),
            now
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(movies)} movies into Snowflake with timestamp {now}")

# Define the DAG
with DAG(
    dag_id="tmdb_to_snowflake_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["tmdb", "snowflake", "versioned"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_popular_movies",
        python_callable=fetch_popular_movies,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_movies_to_snowflake",
        python_callable=load_movies_to_snowflake,
        provide_context=True
    )

    fetch_task >> load_task
