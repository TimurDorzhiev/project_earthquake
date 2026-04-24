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
            logging.error("❌ Найдено {count} NULL значений в поле {field}")

            # Логируем аномалию
            pg_hook.run("""
                insert into data_quality.anomalies(table_name, anomaly_type, description, severity)
                values(%s, %s, %s, %s)
            """, parameters=('earthquake.events',
                              'null_values',
                              f'Поле {field} содержит {count} NULL значений',
                              'CRITICAL'))
        else:
            logging.info(f"✅ Поле {field} не содержит NULL")

    # Сохраняем результаты
    context['ti'].xcom_push(key='null_checks', value=results)
    return results