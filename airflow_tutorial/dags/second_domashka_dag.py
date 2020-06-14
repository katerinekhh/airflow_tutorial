from datetime import timedelta, datetime, date
import requests
import csv
import json
import pandas as pd
import telebot

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import ShortCircuitOperator
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.models import TaskInstance

TOKEN = Variable.get('telegram_token')
CHAT_ID = Variable.get('chat_id')

GOODS_FILE_PATH = '/tmp/goods.csv'
CUSTOMERS_FILE_PATH = '/tmp/customers.csv'
ORDERS_FILE_PATH = '/tmp/orders.csv'
STATUS_FILE_PATH = '/tmp/status.json'
JOINED_DATA_FILE_PATH = '/tmp/final.csv'

GOODS_QUERY = """
SELECT * FROM goods
"""

CUSTOMERS_QUERY = """
SELECT * FROM customers
"""

default_args = {
    'owner': 'bestdoctor',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'shop_dag',
    default_args=default_args,
    description='DAG updates some shopping data',
    schedule_interval='0 10 * * *',
)


def check_postgresql_available(**kwargs):
    """проверяет, что таблица в локальной базе существует"""
    engine = PostgresHook(postgres_conn_id='tutorial_local').get_sqlalchemy_engine()
    try:
        pd.read_sql_table('orders_table', engine)
        return True
    except ValueError:
        return False


def export_postgres_data_to_csv(filepath, query, **kwargs):
    connection = PostgresHook(postgres_conn_id='tutorial_general').get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    with open(filepath, 'w') as csv_file:
        csvwriter = csv.writer(csv_file, delimiter=',')
        csvwriter.writerow(header[0] for header in cursor.description)
        for row in data:
            csvwriter.writerow(row)


def export_http_data_to_file(filepath, conn_id, endpoint, **kwargs):
    http = HttpHook('GET', http_conn_id=conn_id)
    response = http.run(endpoint)
    with open(filepath, 'w') as csv_file:
        csv_file.write(response.text)
        

def get_orders_dict():
    with open(ORDERS_FILE_PATH) as csv_file:
        orders = csv.reader(csv_file, delimiter=',')
        order_by_uuid = {}
        order_by_email = {}
        order_by_good_title = {}
        next(orders)
        for row in orders:
            order_by_uuid[row[1]] = [row[2], row[3], row[4], row[5]]
            order_by_good_title[row[2]] = row[1]
            order_by_email[row[6]] = row[1]

    return order_by_uuid, order_by_good_title, order_by_email


def get_customers_dict():
    with open(CUSTOMERS_FILE_PATH) as csv_file:
        customers = csv.reader(csv_file, delimiter=',')
        customers_data = {}
        next(customers)
        for row in customers:
            customers_data[row[4]] = row[2]
    return customers_data


def get_goods_dict():
    with open(GOODS_FILE_PATH) as csv_file:
        goods = csv.reader(csv_file, delimiter=',')
        goods_data = {}
        next(goods)
        for row in goods:
            goods_data[row[1]] = row[2]
    return goods_data


def get_joined_data(**kwargs):
    orders_data, order_by_good_title, order_by_email = get_orders_dict()
    customers_data = get_customers_dict()
    goods_data = get_goods_dict()
    for email, birth_date in customers_data.items():
        if email in order_by_email:
            order = orders_data[order_by_email[email]]
            today = datetime.today()
            birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
            age = today.year - birth_date.year - (
                (today.month, today.day) < (birth_date.month, birth_date.day)
            )
            order.append(str(age))

    for good, price in goods_data.items():
        if good in order_by_good_title:
            order = orders_data[order_by_good_title[good]]
            price = round(float(price) * int(order[2]), 2)
            order.append(str(price))

    status_data = json.load(open(STATUS_FILE_PATH, 'r'))
    for uuid, info in orders_data.items():
        if uuid in status_data:
            status = status_data[uuid]['success']
            order = orders_data[uuid]
            if status:
                order.append('success')
            else:
                order.append('we are doing our best')
            order.append(date.today())

    return orders_data


def check_if_shop_data_available(task, **kwargs):
    """разрешает грузить данные в таблицу, если больше половины заказов ок (есть 8 значений для колонок)"""
    task_info = kwargs['ti']
    task_info.xcom_push(key='check_data_task_id', value=task_info.task_id)
    task_info.xcom_push(key='dag_id', value=task_info.dag_id)

    orders_data = get_joined_data()
    weird_orders_amount = 0
    for order_id, order_info in orders_data.items():
        if len(order_info) != 8:
            weird_orders_amount += 1

    if weird_orders_amount < len(orders_data):
        return 'load_orders_data'
    else:
        return 'send_telegram_warning'


def send_telegram_warning(**kwargs):
    task_info = kwargs['ti']
    dag_id = task_info.xcom_pull(key='dag_id')
    task_id = task_info.xcom_pull(key='check_data_task_id')
    bot = telebot.TeleBot(TOKEN)
    markup = telebot.types.InlineKeyboardMarkup()
    bot.send_message(
        CHAT_ID, f'broken data :-( dag_id - {dag_id}, task_id - {task_id}', reply_markup=markup)


def write_data_to_csv(**kwargs):
    orders_data = get_joined_data()
    with open(JOINED_DATA_FILE_PATH, 'w') as csv_file:
        headers = [
            'good_title',
            'order_date',
            'amount',
            'name',
            'age',
            'total_price',
            'payment_status',
            'last_modified_at'
        ]
        writer_headers = csv.DictWriter(csv_file, fieldnames=headers)
        writer_headers.writeheader()

        writer = csv.writer(csv_file)
        for info in orders_data.values():
            if len(info) == 8:
                writer.writerow(info)


def load_orders_data_to_csv(**kwargs):
    engine = PostgresHook(postgres_conn_id='tutorial_local').get_sqlalchemy_engine()
    data = pd.read_csv(JOINED_DATA_FILE_PATH)
    df = pd.DataFrame(data)
    df = df.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna(''))
    df.to_sql(
        'orders_table',
        engine,
        if_exists='replace',
        index=False,
    )


postgres_check = ShortCircuitOperator(
    task_id='postgres_check',
    provide_context=False,
    python_callable=check_postgresql_available,
    dag=dag,
)

get_customers_csv=PythonOperator(
    task_id='get_customers_csv',
    provide_context=True,
    python_callable=export_postgres_data_to_csv,
    op_kwargs={'filepath': CUSTOMERS_FILE_PATH, 'query': CUSTOMERS_QUERY},
    dag=dag,
)

get_goods_csv=PythonOperator(
    task_id='get_goods_csv',
    provide_context=True,
    python_callable=export_postgres_data_to_csv,
    op_kwargs={'filepath': GOODS_FILE_PATH, 'query': GOODS_QUERY},
    dag=dag,
)

get_orders_csv=PythonOperator(
    task_id='get_orders_csv',
    provide_context=True,
    python_callable=export_http_data_to_file,
    op_kwargs={
        'filepath': ORDERS_FILE_PATH,
        'conn_id': 'orders_data',
        'endpoint': 'orders.csv'
    },
    dag=dag,
)

get_status_csv=PythonOperator(
    task_id='get_status_csv',
    provide_context=True,
    python_callable=export_http_data_to_file,
    op_kwargs={
        'filepath': STATUS_FILE_PATH,
        'conn_id': 'status_json',
        'endpoint': 'b/5ed7391379382f568bd22822'
    },
    dag=dag,
)

join_orders_info=PythonOperator(
    task_id='join_orders_info',
    provide_context=True,
    python_callable=write_data_to_csv,
    dag=dag,
)

shop_data_check = BranchPythonOperator(
    task_id='shop_data_check',
    python_callable=check_if_shop_data_available,
    provide_context=True,
    dag=dag,
)

telegram_warning=PythonOperator(
    task_id='telegram_warning',
    provide_context=True,
    python_callable=send_telegram_warning,
    dag=dag,
)

load_orders_data=PythonOperator(
    task_id='load_orders_data',
    provide_context=True,
    python_callable=load_orders_data_to_csv,
    dag=dag,
)


postgres_check >> get_customers_csv >> get_goods_csv >> get_orders_csv >> get_status_csv >> join_orders_info
join_orders_info >> telegram_warning >> (shop_data_check, load_orders_data)
