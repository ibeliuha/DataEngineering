import os
os.chdir('/home/user/airflow/dags')
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from paths import DB_CONFIG_PATH, DBSHOT_DATA_PATH
import yaml

def get_config(dbname):
    with open(DB_CONFIG_PATH, 'r') as conf_file:
        conf = yaml.load(conf_file, Loader=yaml.FullLoader)
        conf['dbname'] = dbname
    return conf

def get_all_tables(config_file):
    print(config_file)
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

def get_data(as_of_date, dbname):
    conf = get_config(dbname=dbname)
    tables = get_all_tables(config_file=conf)
    for table in tables:
        with psycopg2.connect(**conf) as con:
            cursor = con.cursor()
            directory = os.path.join(DBSHOT_DATA_PATH, as_of_date)
            if not os.path.exists(directory):
                os.mkdir(directory)
            with open(os.path.join(directory, table[0]+'.csv'), 'w') as output:
                cursor.copy_expert(f'COPY public.{table[0]} TO STDOUT WITH HEADER CSV', output)


with DAG(
    dag_id = 'dbshop_data_collection',
    start_date = datetime(2021, 5, 8),
    schedule_interval = '@once',
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
        task_id = 'get_dbshop_data',
        python_callable=get_data,
        op_kwargs={
            'as_of_date':datetime.now().strftime('%Y-%m-%d'),
            'dbname':'dshop'
        }
    )