import logging
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


def inspect_parquet_schema():
    """
    Проверяет схему Parquet файлов в MinIO
    """
    con = duckdb.connect()

    query = """
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'minio:9000';
    SET s3_access_key_id = 'minioadmin';
    SET s3_secret_access_key = 'minioadmin';
    SET s3_use_ssl = FALSE;

    -- Получаем список всех колонок из Parquet файлов
    SELECT * 
    FROM read_parquet('s3://prod/raw/earthquake/*/*.parquet') 
    LIMIT 1;
    """

    result = con.execute(query).df()
    logging.info(f"📊 Колонки в Parquet файлах: {list(result.columns)}")
    logging.info(f"📊 Пример данных:\n{result.head()}")

    con.close()


with DAG(
        dag_id="inspect_parquet_schema",
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    inspect = PythonOperator(
        task_id="inspect_parquet_schema",
        python_callable=inspect_parquet_schema,
    )

    inspect