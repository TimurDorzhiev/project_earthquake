# dags/earthquake_data_quality.py
import logging
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import numpy as np

default_args = {
    'owner': 't.dorzhiev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_null_values(**context):
    """
    Проверка на NULL значения в критических полях
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    checks = {
        'id': 'SELECT COUNT(*) FROM earthquake.events WHERE id IS NULL',
        'time': 'SELECT COUNT(*) FROM earthquake.events WHERE time IS NULL',
        'mag': 'SELECT COUNT(*) FROM earthquake.events WHERE mag IS NULL',
        'latitude': 'SELECT COUNT(*) FROM earthquake.events WHERE latitude IS NULL',
        'longitude': 'SELECT COUNT(*) FROM earthquake.events WHERE longitude IS NULL'
    }

    results = {}
    for field, query in checks.items():
        count = pg_hook.get_first(query)[0]
        results[field] = count

        if count > 0:
            logging.error(f"❌ Найдено {count} NULL значений в поле {field}")

            # Логируем аномалию
            pg_hook.run("""
                INSERT INTO data_quality.anomalies 
                (table_name, anomaly_type, description, severity)
                VALUES (%s, %s, %s, %s)
            """, parameters=(
                'earthquake.events',
                'null_values',
                f'Поле {field} содержит {count} NULL значений',
                'CRITICAL'
            ))
        else:
            logging.info(f"✅ Поле {field} не содержит NULL")

    # Сохраняем результаты
    context['ti'].xcom_push(key='null_checks', value=results)
    return results


def check_magnitude_range(**context):
    """
    Проверка, что магнитуда в разумных пределах
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    query = """
        SELECT 
            COUNT(CASE WHEN mag < 0 THEN 1 END) as negative_mag,
            COUNT(CASE WHEN mag > 10 THEN 1 END) as too_high_mag,
            MIN(mag) as min_mag,
            MAX(mag) as max_mag,
            AVG(mag) as avg_mag,
            STDDEV(mag) as stddev_mag
        FROM earthquake.events
        WHERE mag IS NOT NULL
    """

    result = pg_hook.get_first(query)
    negative_count, high_count, min_mag, max_mag, avg_mag, stddev_mag = result

    issues = []

    if negative_count > 0:
        issues.append(f"Найдено {negative_count} записей с отрицательной магнитудой")
        logging.error(f"❌ {issues[-1]}")

    if high_count > 0:
        issues.append(f"Найдено {high_count} записей с магнитудой > 10")
        logging.error(f"❌ {issues[-1]}")

    logging.info(f"📊 Статистика магнитуд: min={min_mag}, max={max_mag}, avg={avg_mag:.2f}, std={stddev_mag:.2f}")

    context['ti'].xcom_push(key='magnitude_stats', value={
        'min': min_mag,
        'max': max_mag,
        'avg': avg_mag,
        'stddev': stddev_mag,
        'negative_count': negative_count,
        'high_count': high_count
    })

    return issues


def check_for_duplicates(**context):
    """
    Проверка на дубликаты по ID
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    query = """
        SELECT id, COUNT(*) as dup_count
        FROM earthquake.events
        GROUP BY id
        HAVING COUNT(*) > 1
        LIMIT 10
    """

    duplicates = pg_hook.get_records(query)

    if duplicates:
        logging.error(f"❌ Найдены дубликаты: {len(duplicates)} ID имеют более одной записи")

        for dup_id, count in duplicates[:5]:
            logging.error(f"   ID {dup_id} встречается {count} раз")

        # Логируем аномалию
        pg_hook.run("""
            INSERT INTO data_quality.anomalies 
            (table_name, anomaly_type, description, severity)
            VALUES (%s, %s, %s, %s)
        """, parameters=(
            'earthquake.events',
            'duplicates',
            f'Найдено {len(duplicates)} дублирующихся ID',
            'CRITICAL'
        ))
    else:
        logging.info("✅ Дубликаты не найдены")

    return len(duplicates) if duplicates else 0


def check_geographic_range(**context):
    """
    Проверка географических координат
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    query = """
        SELECT 
            COUNT(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 END) as invalid_lat,
            COUNT(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 END) as invalid_lon,
            COUNT(CASE WHEN depth < 0 OR depth > 700 THEN 1 END) as invalid_depth,
            MIN(latitude) as min_lat,
            MAX(latitude) as max_lat,
            MIN(longitude) as min_lon,
            MAX(longitude) as max_lon
        FROM earthquake.events
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """

    result = pg_hook.get_first(query)
    invalid_lat, invalid_lon, invalid_depth, min_lat, max_lat, min_lon, max_lon = result

    logging.info(f"📊 Географический диапазон: lat [{min_lat:.2f}, {max_lat:.2f}], lon [{min_lon:.2f}, {max_lon:.2f}]")

    if invalid_lat > 0:
        logging.error(f"❌ Найдено {invalid_lat} записей с некорректной широтой")

    if invalid_lon > 0:
        logging.error(f"❌ Найдено {invalid_lon} записей с некорректной долготой")

    if invalid_depth > 0:
        logging.warning(f"⚠️ Найдено {invalid_depth} записей с некорректной глубиной")

    return {
        'invalid_latitude': invalid_lat,
        'invalid_longitude': invalid_lon,
        'invalid_depth': invalid_depth
    }


def check_future_dates(**context):
    """
    Проверка, что даты не в будущем
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    query = """
        SELECT COUNT(*) 
        FROM earthquake.events 
        WHERE time > NOW()
    """

    future_count = pg_hook.get_first(query)[0]

    if future_count > 0:
        logging.error(f"❌ Найдено {future_count} записей с датами в будущем")

        # Показываем примеры
        examples = pg_hook.get_records("""
            SELECT id, time, place 
            FROM earthquake.events 
            WHERE time > NOW() 
            LIMIT 5
        """)

        for ex_id, ex_time, ex_place in examples:
            logging.error(f"   ID: {ex_id}, Время: {ex_time}, Место: {ex_place}")
    else:
        logging.info("✅ Нет записей с датами в будущем")

    return future_count


def detect_outliers(**context):
    """
    Обнаружение статистических выбросов (методом IQR)
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    # Загружаем данные для анализа
    df = pg_hook.get_pandas_df("""
        SELECT mag, depth, latitude, longitude
        FROM earthquake.events
        WHERE mag IS NOT NULL
    """)

    outliers = {}

    for column in ['mag', 'depth']:
        if column in df.columns:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers_count = len(df[(df[column] < lower_bound) | (df[column] > upper_bound)])
            outliers[column] = outliers_count

            if outliers_count > 100:  # Порог
                logging.warning(f"⚠️ Обнаружено {outliers_count} выбросов в колонке {column}")
                logging.warning(f"   Диапазон: [{lower_bound:.2f}, {upper_bound:.2f}]")

    return outliers


def check_row_count_trend(**context):
    """
    Мониторинг изменения количества записей (детектирование аномалий)
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    # Считаем записи за последние 7 дней
    query = """
        SELECT 
            DATE(time) as date,
            COUNT(*) as daily_count
        FROM earthquake.events
        WHERE time >= NOW() - INTERVAL '7 days'
        GROUP BY DATE(time)
        ORDER BY date DESC
    """

    daily_counts = pg_hook.get_records(query)

    if len(daily_counts) >= 2:
        yesterday_count = daily_counts[0][1] if len(daily_counts) > 0 else 0
        avg_count = sum(count for _, count in daily_counts[1:]) / max(len(daily_counts) - 1, 1)

        # Если вчерашнее количество отличается от среднего более чем на 50%
        if yesterday_count < avg_count * 0.5 or yesterday_count > avg_count * 1.5:
            logging.warning(f"⚠️ Аномалия: вчера {yesterday_count} записей, среднее {avg_count:.0f}")

            pg_hook.run("""
                INSERT INTO data_quality.anomalies 
                (table_name, anomaly_type, description, severity)
                VALUES (%s, %s, %s, %s)
            """, parameters=(
                'earthquake.events',
                'row_count_anomaly',
                f'Вчера {yesterday_count} записей, среднее {avg_count:.0f}',
                'WARNING'
            ))

    return daily_counts


def generate_quality_report(**context):
    """
    Генерация отчета о качестве данных
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

    # Собираем все результаты
    null_results = context['ti'].xcom_pull(task_ids='check_null_values', key='null_checks')
    magnitude_stats = context['ti'].xcom_pull(task_ids='check_magnitude_range', key='magnitude_stats')
    duplicate_count = context['ti'].xcom_pull(task_ids='check_duplicates')

    # Общая оценка качества
    total_checks = 0
    failed_checks = 0

    # Подсчитываем failed checks
    if null_results:
        for field, count in null_results.items():
            total_checks += 1
            if count > 0:
                failed_checks += 1

    quality_score = ((total_checks - failed_checks) / total_checks * 100) if total_checks > 0 else 0

    report = f"""
    ========================================
    📊 ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ
    ========================================
    Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    Общая оценка качества: {quality_score:.1f}%
    Проверок выполнено: {total_checks}
    Проверок провалено: {failed_checks}

    📈 Статистика магнитуд:
    - Минимальная: {magnitude_stats.get('min', 'N/A')}
    - Максимальная: {magnitude_stats.get('max', 'N/A')}
    - Средняя: {magnitude_stats.get('avg', 'N/A'):.2f}
    - Стандартное отклонение: {magnitude_stats.get('stddev', 'N/A'):.2f}

    🗑️ Дубликаты: {duplicate_count if duplicate_count else 0}

    ========================================
    """

    logging.info(report)

    # Сохраняем отчет в XCom для других DAG
    context['ti'].xcom_push(key='quality_report', value=report)

    # Можно отправить в Slack/Telegram
    # send_slack_alert(report)

    return report


with DAG(
        dag_id='earthquake_data_quality',
        schedule_interval='0 8 * * *',  # Каждый день в 8 утра
        default_args=default_args,
        catchup=False,
        tags=['quality', 'monitoring', 'earthquake'],
) as dag:
    start = EmptyOperator(task_id='start')

    check_nulls = PythonOperator(
        task_id='check_null_values',
        python_callable=check_null_values,
    )

    check_magnitude = PythonOperator(
        task_id='check_magnitude_range',
        python_callable=check_magnitude_range,
    )

    check_duplicates = PythonOperator(
        task_id='check_duplicates',
        python_callable=check_for_duplicates,
    )

    check_geo = PythonOperator(
        task_id='check_geographic_range',
        python_callable=check_geographic_range,
    )

    check_future = PythonOperator(
        task_id='check_future_dates',
        python_callable=check_future_dates,
    )

    check_outliers = PythonOperator(
        task_id='detect_outliers',
        python_callable=detect_outliers,
    )

    check_trend = PythonOperator(
        task_id='check_row_count_trend',
        python_callable=check_row_count_trend,
    )

    generate_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
    )

    end = EmptyOperator(task_id='end')

    start >> [check_nulls, check_magnitude, check_duplicates, check_geo,
              check_future, check_outliers, check_trend] >> generate_report >> end