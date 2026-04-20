import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


OWNER = "t.dorzhiev"
DAG_ID = "transform_earthquake_s3_to_postgres"

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
    # Получаем год и месяц из контекста (для инкрементальной загрузки)
    # Или загружаем все данные за раз

    con = duckdb.connect()

    try:
        # Создаем таблицу в PostgreSQL
        create_table_query = """
        ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);
        
        CREATE SCHEMA IF NOT EXISTS pg.earthquake;
        
        -- Удаляем старую таблицу если есть
        DROP TABLE IF EXISTS pg.earthquake.events;
       
       -- Создаем новую таблицу с правильной структурой
        CREATE TABLE pg.earthquake.events (
            id VARCHAR(50) PRIMARY KEY,
            time TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE,
            depth DOUBLE,
            mag DOUBLE,
            magType VARCHAR(10),
            nst INTEGER,
            gap DOUBLE,
            dmin DOUBLE,
            rms DOUBLE,
            net VARCHAR(10),
            updated TIMESTAMP,
            place TEXT,
            type VARCHAR(30),
            horizontalError DOUBLE,
            depthError DOUBLE,
            magError DOUBLE,
            magNst INTEGER,
            status VARCHAR(20),
            locationSource VARCHAR(20),
            magSource VARCHAR(20),
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        con.execute(create_table_query)
        logging.info("✅ Таблица создана в PostgreSQL")

        # Загружаем данные
        insert_query = """
                ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);

                INSERT INTO pg.earthquake.events
                SELECT 
                    id,
                    time::TIMESTAMP as time,
                    latitude,
                    longitude,
                    depth,
                    mag,
                    magType,
                    nst,
                    gap,
                    dmin,
                    rms,
                    net,
                    updated::TIMESTAMP as updated,
                    place,
                    type,
                    horizontalError,
                    depthError,
                    magError,
                    magNst,
                    status,
                    locationSource,
                    magSource,
                    CURRENT_TIMESTAMP as ingestion_time
                FROM read_parquet('s3://prod/raw/earthquake/*/*.parquet')
                ON CONFLICT (id) DO UPDATE SET
                    mag = EXCLUDED.mag,
                    place = EXCLUDED.place,
                    updated = EXCLUDED.updated,
                    status = EXCLUDED.status,
                    ingestion_time = CURRENT_TIMESTAMP;
                """

        con.execute(insert_query)
        logging.info("✅ Данные загружены в PostgreSQL")

        # Проверяем количество записей
        count_query = """
                ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);
                SELECT COUNT(*) FROM pg.earthquake.events;
                """
        count = con.execute(count_query).fetchone()
        logging.info(f"📊 Всего записей в PostgreSQL: {count[0]}")

        # Показываем статистику по магнитудам
        stats_query = """
                ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);
                SELECT 
                    COUNT(*) as total_events,
                    MIN(mag) as min_magnitude,
                    AVG(mag) as avg_magnitude,
                    MAX(mag) as max_magnitude,
                    COUNT(DISTINCT place) as unique_places
                FROM pg.earthquake.events
                WHERE mag IS NOT NULL;
                """
        stats = con.execute(stats_query).fetchone()
        logging.info(f"📊 Статистика: {stats}")

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


