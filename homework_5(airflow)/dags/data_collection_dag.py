import os
os.chdir('/home/user/airflow/dags')
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
import errno
import requests
import yaml
import json
from datetime import datetime, timedelta
from Config import Config
from paths import OUT_OF_STOCK_DATA_PATH, LOG_PATH
import logging
#some piece of code which resolve the problem when loger doesn't write anything to a file
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=LOG_PATH, level=logging.WARN)

def get_data(date):
    conf = Config()
    result = requests.get(
        url=conf.endpoint_url,
        data=json.dumps({'date':date}),
        headers=conf.headers
        )
    if result.status_code != 200:
        logging.error(f"time={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=Can not get data from API for {date}|error={result.text}")
        raise AirflowException(f'request date:{date}, error:{result.text}')
    write_data(result.json(), date)


def write_data(data, date):
    with open(os.path.join(OUT_OF_STOCK_DATA_PATH, date+'.json'), 'w') as f:
        json.dump(data, f)

with DAG(
    dag_id = 'out_of_stock_data_collection',
    start_date = datetime(2021, 4, 6),
    schedule_interval = '@daily',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        } 
    ) as dag:
    
    get_data = PythonOperator(
        task_id = 'get_out_of_stock_data',
        python_callable=get_data,
        op_kwargs={'date':'{{ds}}'}
    )