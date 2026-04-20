# dags/transform_s3_to_postgres_fixed.py
import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "t.dorzhiev"
DAG_ID = "transform_earthquake_s3_to_postgres_fixed"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2015, 1, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def load_parquet_to_postgres(**context):
    """
    Читает Parquet файлы из MinIO и загружает в PostgreSQL
    """
    con = duckdb.connect()

    try:
        # Сначала проверим, какие колонки есть в данных
        schema_query = """
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = 'minioadmin';
        SET s3_secret_access_key = 'minioadmin';
        SET s3_use_ssl = FALSE;

        SELECT * 
        FROM read_parquet('s3://prod/raw/earthquake/*/*.parquet') 
        LIMIT 1;
        """

        sample_df = con.execute(schema_query).df()
        columns = list(sample_df.columns)
        logging.info(f"📊 Найденные колонки: {columns}")

        # Определяем соответствие колонок (маппинг)
        column_mapping = {
            'earthquake_id': ['id', 'event_id', 'quake_id'],
            'magnitude': ['mag', 'magnitude', 'mag_value'],
            'place': ['place', 'location', 'region'],
            'timestamp_ms': ['time', 'timestamp', 'event_time'],
            'longitude': ['longitude', 'lon', 'lng'],
            'latitude': ['latitude', 'lat'],
            'depth_km': ['depth', 'depth_km', 'depth_km_value'],
        }

        # Строим SELECT динамически
        select_parts = []

        for target_col, possible_names in column_mapping.items():
            found = None
            for pname in possible_names:
                if pname in columns:
                    found = pname
                    break

            if found:
                if target_col == 'timestamp_ms':
                    select_parts.append(f"{found} as {target_col}")
                elif target_col in ['magnitude', 'depth_km']:
                    select_parts.append(f"CAST({found} AS DOUBLE) as {target_col}")
                else:
                    select_parts.append(f"{found} as {target_col}")
            else:
                select_parts.append(f"NULL as {target_col}")
                logging.warning(f"⚠️ Колонка {target_col} не найдена, будет NULL")

        # Добавляем все остальные колонки как есть
        for col in columns:
            if col not in [item for sublist in column_mapping.values() for item in sublist]:
                select_parts.append(col)

        select_clause = ",\n            ".join(select_parts)

        # Создаем таблицу в PostgreSQL если не существует
        create_table_query = """
        ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);

        CREATE SCHEMA IF NOT EXISTS pg.earthquake;

        CREATE TABLE IF NOT EXISTS pg.earthquake.events_raw (
            id VARCHAR(50),
            mag DOUBLE,
            place TEXT,
            time BIGINT,
            lon DOUBLE,
            lat DOUBLE,
            depth DOUBLE,
            url TEXT,
            detail TEXT,
            felt INTEGER,
            cdi DOUBLE,
            mmi DOUBLE,
            alert VARCHAR(20),
            status VARCHAR(20),
            tsunami INTEGER,
            sig INTEGER,
            net VARCHAR(10),
            code VARCHAR(20),
            ids TEXT,
            sources TEXT,
            types TEXT,
            nst INTEGER,
            dmin DOUBLE,
            rms DOUBLE,
            gap DOUBLE,
            magType VARCHAR(10),
            type VARCHAR(30),
            title TEXT,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        con.execute(create_table_query)
        logging.info("✅ Таблица создана в PostgreSQL")

        # Загружаем данные
        insert_query = f"""
        ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);

        INSERT INTO pg.earthquake.events_raw
        SELECT 
            {select_clause},
            CURRENT_TIMESTAMP as ingestion_time
        FROM read_parquet('s3://prod/raw/earthquake/*/*.parquet')
        ON CONFLICT (id) DO UPDATE SET
            mag = EXCLUDED.mag,
            place = EXCLUDED.place,
            ingestion_time = CURRENT_TIMESTAMP;
        """

        con.execute(insert_query)
        logging.info("✅ Данные загружены в PostgreSQL")

        # Проверяем количество
        count_query = """
        ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);
        SELECT COUNT(*) FROM pg.earthquake.events_raw;
        """
        count = con.execute(count_query).fetchone()
        logging.info(f"📊 Всего записей в PostgreSQL: {count[0]}")

    except Exception as e:
        logging.error(f"❌ Ошибка загрузки: {e}")
        raise
    finally:
        con.close()


with DAG(
        dag_id=DAG_ID,
        schedule_interval="@once",
        default_args=args,
        tags=["transform", "postgres", "earthquake"],
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    load_to_postgres = PythonOperator(
        task_id="load_parquet_to_postgres",
        python_callable=load_parquet_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> load_to_postgres >> end