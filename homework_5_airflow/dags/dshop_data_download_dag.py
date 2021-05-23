import os
os.chdir('/home/user/airflow/dags')
import psycopg2
import hdfs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from paths import CONFIG_PATH
import yaml
#constant definition
DBNAME='dshop_bu'

def get_db_config(conf_id='DB'):
    with open(CONFIG_PATH, 'r') as conf_file:
        conn_id = yaml.load(conf_file, Loader=yaml.FullLoader)['app'][conf_id]['airflow_conn_id']
        conn = BaseHook.get_connection(conn_id)
    
        conf = dict()
        conf['user'] = conn.login
        conf['password'] = conn.password
        conf['host'] = conn.host
        conf['port'] = conn.port
        conf['dbname'] = DBNAME
    return conf

def get_hdfs_connection(conf_id='HDFS'):
    with open(CONFIG_PATH, 'r') as conf_file:
        conn_id = yaml.load(conf_file, Loader=yaml.FullLoader)['app'][conf_id]['airflow_conn_id']
        conn = BaseHook.get_connection(conn_id)
        url = conn.host+':'+str(conn.port)+'/'
        user = conn.login
    return url, user

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
    with psycopg2.connect(**config) as con:
        cursor = con.cursor()
        directory = DBNAME+'/'+table
        #hdfs connection
        url, user = get_hdfs_connection()
        client = hdfs.InsecureClient(url, user=user)
        with client.write(directory+'/'+as_of_date+'.csv') as output:
            cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', output)

def generate_tasks(dag, table, as_of_date, config_file):
    mkdir_op = BashOperator(
        task_id = table+'_mkdir',
        bash_command = f'''hadoop fs -mkdir -p {DBNAME}/{table}'''
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
    conf = get_db_config()
    tables = get_all_tables(config_file=conf)
    for table in tables:
        globals()[table[0]+'_mkdir'], globals()[table[0]] = generate_tasks(
            dag=dag,
            table=table[0],
            as_of_date=datetime.now().strftime('%Y-%m-%d'),
            config_file=conf)
        globals()[table[0]+'_mkdir']>>globals()[table[0]]

