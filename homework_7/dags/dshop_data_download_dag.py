import os
os.chdir('/home/user/airflow/dags')
import psycopg2
import hdfs
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import DummyOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from connections import get_db_config, get_hdfs_config
import yaml
from paths import BRONZE_DIR,SILVER_DIR
import logging
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
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data extraction|status=started")
    try:
        with psycopg2.connect(**config) as con:
            cursor = con.cursor()
            directory = '/'.join([BRONZE_DIR,DBNAME,table])
            #hdfs connection
            url, user = get_hdfs_config(conf_type='connection')
            client = hdfs.InsecureClient(url, user=user)
            with client.write(directory+'/'+as_of_date+'.csv') as output:
                cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', output)
                logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data extraction|status=succeeded")
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data extraction|status=failed|error={e}")

def clean_data(as_of_date, table):
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=started")
    try:
        spark = SparkSession\
            .builder\
            .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
            .appName("cleaning_dshop_bronze_data")\
            .getOrCreate()
        data = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv(f"{BRONZE_DIR}/{DBNAME}/{table}/{as_of_date}.csv")
        #partition data by parameter in config.yml. If there is no table specified - partitioning is no applied
        try:
            partition = get_hdfs_config(conf_type='partitions')[table]
            data.write\
                .partitionBy(partition)\
                .parquet(f'{SILVER_DIR}/{DBNAME}/{table}', mode='overwrite')
        except KeyError:
            data.write\
                .parquet(f'{SILVER_DIR}/{DBNAME}/{table}', mode='overwrite')
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=failed|error={e}")
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=succeeded")


def generate_bronze_tasks(dag, table, as_of_date, config_file):
    mkdir_op = BashOperator(
        task_id = table+'_mkdir_bronze',
        bash_command = f'''hadoop fs -mkdir -p {BRONZE_DIR}/{DBNAME}/{table}'''
    )

    py_op = PythonOperator(
        task_id = table+'_bronze',
        python_callable=get_data,
        op_kwargs={
            'as_of_date':as_of_date,
            'table':table,
            'config':config_file
        },
        dag=dag,
        trigger_rule='all_success'
    )
    return (mkdir_op, py_op)

def generate_silver_tasks(dag, table, as_of_date):
    mkdir_op = BashOperator(
        task_id = table+'_mkdir_silver',
        bash_command = f'''hadoop fs -mkdir -p {SILVER_DIR}/{DBNAME}/{table}''',
        trigger_rule='all_success'
    )

    py_op = PythonOperator(
        task_id = table+'_silver',
        python_callable=clean_data,
        op_kwargs={
            'as_of_date':as_of_date,
            'table':table
        },
        dag=dag,
        trigger_rule='all_success'
    )
    return (mkdir_op, py_op)

with DAG(
    dag_id = 'dbshop_data_collection',
    start_date = datetime(2021, 5, 28),
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
        dag=dag,
        trigger_rule='none_skipped'
    )
    silver_dummy = DummyOperator(
        task_id = 'silver_layer',
        dag=dag,
        trigger_rule='none_skipped'
    )

    conf = get_db_config(dbname=DBNAME)
    tables = get_all_tables(config_file=conf)
    for table in tables:
        #BRONZE TASKS
        globals()[table[0]+'_mkdir_bronze'], globals()[table[0]+'_bronze'] = generate_bronze_tasks(
            dag=dag,
            table=table[0],
            as_of_date=datetime.now().strftime('%Y-%m-%d'),
            config_file=conf)
        #SILVER TASKS
        globals()[table[0]+'_mkdir_silver'], globals()[table[0]+'_silver'] = generate_silver_tasks(
            dag=dag,
            table=table[0],
            as_of_date=datetime.now().strftime('%Y-%m-%d')
            )
        
        globals()[table[0]+'_mkdir_bronze']>>globals()[table[0]+'_bronze']>>bronze_dummy>>globals()[table[0]+'_mkdir_silver']>>globals()[table[0]+'_silver']>>silver_dummy
    
    

