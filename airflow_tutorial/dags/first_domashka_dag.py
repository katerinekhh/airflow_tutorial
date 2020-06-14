from datetime import timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


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
    description='DAG updates covid info data daily',
    schedule_interval='0 9 * * *',
)


def get_covid_data_csv_from_ya_api(**kwargs):
    result = requests.get(
        'https://yastat.net/s3/milab/2020/covid19-stat/data/data_struct_10.json?v=timestamp')
    covid_data = eval(result.text)

    russia_stat_struct = covid_data['russia_stat_struct']
    dates = russia_stat_struct['dates']
    data = russia_stat_struct['data']

    covid_russia_data = []
    for number, region_data in data.items():
        region_name = region_data['info']['name']
        infected = region_data['cases']
        recovered = region_data['cured']
        dead = region_data['deaths']
        for index in range(len(dates)):
            region_date_data = {}
            region_date_data['date'] = dates[index]
            region_date_data['region'] = region_name
            if index < len(infected):
                region_date_data['infected'] = int(infected[index]['v'])
            if index < len(recovered):
                region_date_data['recovered'] = int(recovered[index]['v'])
            if index < len(dead):
                region_date_data['dead'] = int(dead[index]['v'])

            covid_russia_data.append(region_date_data)

    covid_russia_dataframe = pd.DataFrame(covid_russia_data)
    covid_russia_dataframe.to_csv(
        '/var/www/html/csv/covid_data.csv', index=False)


get_covid_data_csv = PythonOperator(
    task_id='get_covid_data_csv',
    provide_context=True,
    python_callable=get_covid_data_csv_from_ya_api,
    dag=dag,
)
