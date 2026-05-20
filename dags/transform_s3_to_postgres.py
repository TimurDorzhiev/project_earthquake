import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

OWNER = "t.dorzhiev"
DAG_ID = "transform_earthquake_unified"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def load_incremental_data(**context):
    """
    Идемпотентная загрузка данных.
    Загружает только новые данные, которых ещё нет в PostgreSQL.
    """
    con = duckdb.connect()

    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE S3,
                PROVIDER config,
                KEY_ID '{ACCESS_KEY}',
                SECRET '{SECRET_KEY}',
                ENDPOINT 'minio:9000',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)
        logging.info("✅ Секрет создан")

        con.execute("DETACH DATABASE IF EXISTS pg;")
        con.execute("""
            ATTACH 'dbname=postgres user=postgres password=postgres host=postgres_dwh port=5432' AS pg (TYPE postgres);
        """)
        logging.info("✅ Подключение к PostgreSQL")

        # Создаем таблицу если не существует
        con.execute("""
            CREATE SCHEMA IF NOT EXISTS pg.earthquake;

            CREATE TABLE IF NOT EXISTS pg.earthquake.events (
                id VARCHAR(50) PRIMARY KEY,
                time TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                depth DOUBLE,
                mag DOUBLE,
                place TEXT,
                status VARCHAR(20),
                year INTEGER,
                month INTEGER,
                ingestion_time TIMESTAMP
            );
        """)
        logging.info("✅ Таблица создана/существует")

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # ============================================
        # ИСПРАВЛЕННЫЙ ЗАПРОС - используем earthquake_id вместо id
        # ============================================
        query = f"""
        INSERT INTO pg.earthquake.events
        SELECT 
            earthquake_id as id,
            to_timestamp(timestamp_ms/1000) as time,
            latitude,
            longitude,
            depth_km as depth,
            magnitude as mag,
            place,
            status,
            EXTRACT(YEAR FROM to_timestamp(timestamp_ms/1000)) as year,
            EXTRACT(MONTH FROM to_timestamp(timestamp_ms/1000)) as month,
            '{current_time}'::TIMESTAMP as ingestion_time
        FROM read_parquet('s3://prod/raw/earthquake/**/*.parquet', union_by_name=True)
        WHERE magnitude >= 4.5
        AND earthquake_id NOT IN (
            SELECT id FROM pg.earthquake.events
        )
        ON CONFLICT (id) DO NOTHING;
        """

        logging.info("🔄 Выполняется загрузка данных...")
        con.execute(query)

        # Получаем количество загруженных записей
        count_result = con.execute("""
            SELECT COUNT(*) FROM pg.earthquake.events
        """).fetchone()

        logging.info(f"📊 Всего записей в PostgreSQL: {count_result[0]}")

        # Статистика по годам
        stats = con.execute("""
            SELECT 
                year,
                COUNT(*) as cnt
            FROM pg.earthquake.events
            GROUP BY year
            ORDER BY year;
        """).fetchall()

        logging.info("📊 Результат по годам:")
        if stats:
            for year, cnt in stats:
                logging.info(f"  {int(year)}: {cnt} записей")
        else:
            logging.info("  Нет данных")

        con.execute("DETACH DATABASE pg;")
        logging.info("✅ Готово!")

    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
        raise
    finally:
        con.close()


with DAG(
        dag_id=DAG_ID,
        schedule_interval="@daily",
        default_args=args,
        catchup=False,
) as dag:
    load = PythonOperator(
        task_id="load_incremental_data",
        python_callable=load_incremental_data,
    )

    load