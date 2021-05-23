import os
os.chdir('/home/user/airflow/dags')
import psycopg2
import hdfs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import DummyOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from connections import get_db_config, get_hdfs_connection
import yaml
from paths import BRONZE_DIR, LOG_PATH
import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=os.path.join(LOG_PATH,'dshop_data_collection.log'), level=logging.INFO)
#constant definition
DBNAME='dshop_bu'

def get_all_tables(config_file):
    query = '''
                select table_name 
                from information_schema.tables 
                where table_schema = 'public'
            '''
    with psycopg2.connect(**config_file) as con:
        cursor = con.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
    return tables

def get_data(as_of_date, table, config):
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=started data extraction from table {table}")
    try:
        with psycopg2.connect(**config) as con:
            cursor = con.cursor()
            directory = BRONZE_DIR+DBNAME+'/'+table
            #hdfs connection
            url, user = get_hdfs_connection()
            client = hdfs.InsecureClient(url, user=user)
            with client.write(directory+'/'+as_of_date+'.csv') as output:
                cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', output)
                logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=finished data extraction from table {table}")
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|message=data extraction for {table} failed|error={e}")

def generate_tasks(dag, table, as_of_date, config_file):
    mkdir_op = BashOperator(
        task_id = table+'_mkdir',
        bash_command = f'''hadoop fs -mkdir -p {BRONZE_DIR}{DBNAME}/{table}'''
    )

    py_op = PythonOperator(
        task_id = table,
        python_callable=get_data,
        op_kwargs={
            'as_of_date':as_of_date,
            'table':table,
            'config':config_file
        },
        dag=dag
    )
    return (mkdir_op, py_op)

with DAG(
    dag_id = 'dbshop_data_collection',
    start_date = datetime(2021, 5, 22),
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

    bronze_dummy = DummyOperator(
        task_id = 'bronze_layer',
        dag=dag
    )

    conf = get_db_config(dbname=DBNAME)
    tables = get_all_tables(config_file=conf)
    for table in tables:
        globals()[table[0]+'_mkdir'], globals()[table[0]] = generate_tasks(
            dag=dag,
            table=table[0],
            as_of_date=datetime.now().strftime('%Y-%m-%d'),
            config_file=conf)
        globals()[table[0]+'_mkdir']>>globals()[table[0]]>>bronze_dummy
    
    

