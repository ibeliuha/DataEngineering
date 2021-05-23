import os
os.chdir('/home/user/airflow/dags')
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
import errno
import requests
import json
import hdfs
from datetime import datetime, timedelta
from Config import Config
from connections import get_hdfs_connection
from paths import LOG_PATH, BRONZE_DIR
import logging
#some piece of code which resolve the problem when loger doesn't write anything to a file
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=os.path.join(LOG_PATH, 'out_of_stock_data_collection.log'), level=logging.INFO)
DIRECTORY = BRONZE_DIR+'out_of_stock'

def get_data(date):
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=started data extraction from API for {date}")
    conf = Config()
    result = requests.get(
        url=conf.endpoint_url,
        data=json.dumps({'date':date}),
        headers=conf.headers
        )
    if result.status_code != 200:
        logging.error(f"time={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=Can not get data from API for {date}|error={result.text}")
        raise AirflowException(f'request date:{date}, error:{result.text}')
    else:
        write_data(result.json(), date)
        logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=finished data extraction from API for {date}")


def write_data(data, date):
    url, user = get_hdfs_connection()
    client = hdfs.InsecureClient(url, user=user)
    with client.write(DIRECTORY+'/'+date+'.json', encoding='utf-8', overwrite=True) as output:
        json.dump(data, output)
        #cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', output)

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

    mkdir_task = BashOperator(
        task_id = 'mkdir',
        bash_command = f'''hadoop fs -mkdir -p {DIRECTORY}'''
    )
    
    get_data_task = PythonOperator(
        task_id = 'load_to_bronze_out_of_stock_data',
        python_callable=get_data,
        op_kwargs={'date':'{{ds}}'}
    )
    mkdir_task>>get_data_task