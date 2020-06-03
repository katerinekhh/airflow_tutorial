from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'bestdoctor',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid_dag',
    default_args=default_args,
    description='DAG updates corona info data daily',
    schedule_interval='0 12 * * *',
)
