from datetime import timedelta
from random import randint
import telebot
from time import sleep

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

CHAT_ID = Variable.get('chat_id')
TOKEN = Variable.get('telegram_token')


def send_telegram_notification(warning_message: str, task_id: str):
    bot = telebot.TeleBot(TOKEN)
    markup = telebot.types.InlineKeyboardMarkup()
    bot.send_message(
        CHAT_ID, f'task id: {task_id}, warning: {warning_message} :-(', reply_markup=markup)


def send_warning_on_fail_callback(context):
    task_id = context['task_instance_key_str'].split('__')[1]
    warning_message = 'TASK FAILED'
    send_telegram_notification(warning_message, task_id)


def send_warning_on_sla_miss_callback(context):
    task_id = context['task_instance_key_str'].split('__')[1]
    warning_message = 'TASK FAILED'
    send_telegram_notification(warning_message, task_id)


default_args = {
    'owner': 'bestdoctor',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 0,
    'on_failure_callback': send_warning_on_fail_callback,
    'sla_miss_callback': send_warning_on_sla_miss_callback,
    'sla': timedelta(seconds=10),
}

dag = DAG(
    'kanareika_dag',
    default_args=default_args,
    description='DAG runs random kanareika every 5 minutes',
    schedule_interval='*/5 * * * *',
)


def check_random_numbers(**kwargs):
    number = randint(1, 5)
    if number == 5:
        sleep(20)
    elif number == 4:
        raise Exception('unlucky random number..')
    else:
        return 0


check_random_numbers = PythonOperator(
    task_id='check_random_numbers',
    python_callable=check_random_numbers,
    provide_context=True,
    dag=dag,
)
