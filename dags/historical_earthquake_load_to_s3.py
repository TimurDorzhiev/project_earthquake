import logging
import duckdb
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "t.dorzhiev"
DAG_ID = "historical_earthquake_load_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# Историческая загрузка данных о землетрясениях
## Период: 2015-2025
## Магнитуда: > 4.5
## Источник: USGS Earthquake API
"""

SHORT_DESCRIPTION = "Загрузка исторических данных о землетрясениях (2015-2025, M>4.5)"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2015, 1, 1, tz="UTC"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def get_date_range(**context):
    """
    Получает интервал дат из контекста Airflow
    """
    start_date = context["data_interval_start"]
    end_date = context["data_interval_end"]
    return start_date, end_date


def load_monthly_earthquake_data(**context):
    """
    Загружает данные о землетрясениях за конкретный месяц
    Фильтр: магнитуда > 4.5
    """
    start_date, end_date = get_date_range(**context)

    # Корректируем end_date, чтобы не захватывать следующий месяц
    end_date_corrected = end_date - timedelta(days=1)

    # Форматируем даты для API
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date_corrected.strftime('%Y-%m-%d')

    year = start_date.year
    month = start_date.month

    logging.info(f"💻 Загружаю данные за {start_str} - {end_str} (M > 4.5)")

    con = duckdb.connect()

    try:
        # Формируем URL с фильтром по магнитуде
        url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_str}&endtime={end_str}&minmagnitude=4.5&limit=20000"

        # Сначала проверим, сколько данных пришло из API
        count_query = f"""
        INSTALL json;
        LOAD json;

        WITH raw_data AS (
            SELECT unnest(features) as feature
            FROM read_json_auto('{url}')
        )
        SELECT COUNT(*) as cnt
        FROM raw_data
        WHERE CAST(feature->'properties'->>'mag' AS DOUBLE) >= 4.5;
        """

        result_count = con.execute(count_query).fetchone()
        record_count = result_count[0] if result_count else 0

        if record_count == 0:
            logging.warning(f"⚠️ Нет данных с магнитудой > 4.5 за период {start_str} - {end_str}")
            return

        logging.info(f"📊 Найдено {record_count} записей для загрузки")

        # Теперь загружаем данные
        query = f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL json;
        LOAD json;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        -- Распарсиваем JSON и извлекаем нужные поля
        COPY (
            SELECT 
                feature->>'id' as earthquake_id,
                CAST(feature->'properties'->>'mag' AS DOUBLE) as magnitude,
                feature->'properties'->>'place' as place,
                CAST(feature->'properties'->>'time' AS BIGINT) as timestamp_ms,
                to_timestamp(CAST(feature->'properties'->>'time' AS BIGINT)/1000) as earthquake_time,
                CAST(feature->'geometry'->'coordinates'->>0 AS DOUBLE) as longitude,
                CAST(feature->'geometry'->'coordinates'->>1 AS DOUBLE) as latitude,
                CAST(feature->'geometry'->'coordinates'->>2 AS DOUBLE) as depth_km,
                feature->'properties'->>'status' as status,
                CAST(feature->'properties'->>'sig' AS INTEGER) as significance,
                feature->'properties'->>'magType' as magnitude_type,
                CAST(feature->'properties'->>'nst' AS INTEGER) as num_stations,
                CAST(feature->'properties'->>'gap' AS DOUBLE) as azimuthal_gap,
                {year} as year,
                {month} as month,
                '{start_str}' as load_date
            FROM (
                SELECT unnest(features) as feature
                FROM read_json_auto('{url}')
            )
            WHERE CAST(feature->'properties'->>'mag' AS DOUBLE) >= 4.5
        ) TO 's3://prod/{LAYER}/{SOURCE}/year={year}/month={month:02d}/earthquakes_{start_str}.parquet'
        (FORMAT 'parquet');
        """

        con.execute(query)
        logging.info(
            f"✅ Данные сохранены в s3://prod/{LAYER}/{SOURCE}/year={year}/month={month:02d}/earthquakes_{start_str}.parquet")

        # Проверяем, что файл создался
        check_query = f"""
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        SELECT COUNT(*) FROM read_parquet('s3://prod/{LAYER}/{SOURCE}/year={year}/month={month:02d}/earthquakes_{start_str}.parquet')
        """

        verify_count = con.execute(check_query).fetchone()
        logging.info(f"✅ Подтверждено {verify_count[0]} записей в файле")

    except Exception as e:
        logging.error(f"❌ Ошибка загрузки за {start_str}: {e}")
        raise
    finally:
        con.close()


# Настраиваем DAG
with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 5 1 * *",  # Каждый месяц 1-го числа в 5:00
        default_args=args,
        tags=["s3", "raw", "historical", "earthquake"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
        catchup=True,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    load_earthquake_data = PythonOperator(
        task_id="load_monthly_earthquake_data",
        python_callable=load_monthly_earthquake_data,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> load_earthquake_data >> end