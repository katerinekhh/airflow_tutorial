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
    schedule_interval='0 12 * * *',
)


def get_covid_data_csv_from_ya_api(**kwargs):
    result = requests.get('https://yastat.net/s3/milab/2020/covid19-stat/data/data_struct_10.json?v=timestamp')
    covid_data = eval(result.text)

    russia_stat_struct = covid_data['russia_stat_struct']
    dates = russia_stat_struct['dates']
    data = russia_stat_struct['data']

    covid_russia_data = []
    for number, region_data in data.items():
        if 'full_name' in region_data['info'].keys():
            region_name = region_data['info']['full_name']
        else:
            continue
        infected = region_data['cases']
        recovered = region_data['cured']
        dead = region_data['cured']
        for index in range(len(dates)):
            region_date_data = {}
            region_date_data['date'] = dates[index]
            region_date_data['region'] = region_name
            region_date_data['infected'] = infected[index]['v']
            region_date_data['recovered'] = recovered[index]['v']
            region_date_data['dead'] = dead[index]['v']

            covid_russia_data.append(region_date_data)

    covid_russia_dataframe = pd.DataFrame(covid_russia_data)
    covid_russia_dataframe.to_csv('covid_data.csv', index=False)


get_covid_data_csv= PythonOperator(
    task_id='get_covid_data_csv',
    provide_context=True,
    python_callable=get_covid_data_csv_from_ya_api,
    dag=dag,
)
