from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

from airflow.models import Variable

from click_message_button_sensor import ClickMessageButtonSensor
from create_json_for_airtable_operator import CreateJsonForAirtableOperator
from load_data_to_airtable_operator import LoadDataToAirtableOperator
from send_tg_button_message_operator import SendTelegramButtonMessageOperator

TOKEN = Variable.get('telegram_token')
CHAT_ID = Variable.get('chat_id')
AIRTABLE_API_KEY = Variable.get('airtable_api_key')
AIRTABLE_TG_TABLE_KEY = Variable.get('airtable_tg_table_key')
AIRTABLE_TG_TABLE_NAME = Variable.get('airtable_tg_table_name')
TEXT = 'Поехали'
MESSAGE_ID_FILE_PATH = 'message_id_file.txt'
UPDATE_FILE_PATH = 'update_file.json'


default_args = {
    'owner': 'bestdoctor',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'telegram_dag',
    default_args=default_args,
    description='DAG sends weird telegram message',
    schedule_interval='0 10 * * *',
)

send_tg_message = SendTelegramButtonMessageOperator(
    task_id = 'send_tg_message',
    tg_token = TOKEN,
    chat_id = CHAT_ID,
    message_text = TEXT,
    message_id_filepath = MESSAGE_ID_FILE_PATH,
    dag = dag,
)

wait_for_click = ClickMessageButtonSensor(
    task_id = 'wait_for_click',
    endpoint = f'bot{TOKEN}/getUpdates',
    http_conn_id = 'telegram_updates',
    message_id_filepath = MESSAGE_ID_FILE_PATH,
    poke_interval = 60,
    retries = 10,
    dag = dag,
)

get_update_csv = CreateJsonForAirtableOperator(
    task_id = 'get_update_csv',
    endpoint = f'bot{TOKEN}/getUpdates',
    http_conn_id = 'telegram_updates',
    message_id_filepath = MESSAGE_ID_FILE_PATH,
    update_filepath = UPDATE_FILE_PATH,
    dag = dag,
)

load_update_to_airtable = LoadDataToAirtableOperator(
    task_id = 'load_update_to_airtable',
    http_conn_id = 'airtable',
    json_file_path = UPDATE_FILE_PATH,
    airtable_api_key = AIRTABLE_API_KEY,
    table_key = AIRTABLE_TG_TABLE_KEY,
    table_name = AIRTABLE_TG_TABLE_NAME,
    dag = dag,
)

send_tg_message >> wait_for_click >> get_update_csv >> load_update_to_airtable
