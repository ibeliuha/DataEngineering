import os
os.chdir('/home/user/airflow/dags')
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
import errno
import requests
import json
import hdfs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType, IntegerType, StructType, StructField
from datetime import datetime, timedelta
from Config import Config
from connections import get_hdfs_config, get_dw_config
from paths import BRONZE_DIR, SILVER_DIR
import logging
#define an API name
API_NAME='out_of_stock'


def get_data(date):
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data extraction|status=started")
    conf = Config()
    result = requests.get(
        url=conf.endpoint_url,
        data=json.dumps({'date':date}),
        headers=conf.headers
        )
    if result.status_code != 200:
        logging.error(f"time={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data extraction|status=failed|error={result.text}|status_code={result.status_code}")
        raise AirflowException()
    else:
        write_data(result.json(), date)
        logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data extraction|status=succeeded")

def write_data(data, date):
    directory = f'{BRONZE_DIR}/{API_NAME}'
    os.system(f'hadoop fs -mkdir -p {directory}') #create directory if not exists
    
    url, user = get_hdfs_config(conf_type='connection')
    client = hdfs.InsecureClient(url, user=user)
    with client.write(f"{directory}/{date}.json", encoding='utf-8', overwrite=True) as output:
        json.dump(data, output)

def clean_data(date):
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data cleaning|status=started")
    directory = f'{SILVER_DIR}/{API_NAME}'
    os.system(f'hadoop fs -mkdir -p {directory}') #create directory if not exists
    try:
        spark = SparkSession\
            .builder\
            .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
            .appName("cleaning_out_of_stock_bronze_data")\
            .getOrCreate()
        schema = StructType(
            [
                StructField("date", DateType(), True),
                StructField("product_id", IntegerType(), True)
            ]
        ) 
        data = spark.read.schema(schema).json(f"{BRONZE_DIR}/{API_NAME}/{date}.json")
        data = data\
                .filter(F.col('date').isNotNull() & F.col('product_id').isNotNull())\
                .dropDuplicates()

        partition = get_hdfs_config(conf_type='partitions')['out_of_stock']
        data.write\
            .partitionBy(partition)\
            .parquet(directory, mode='append')
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data cleaning|status=failed|error={e}")
        raise AirflowException()
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|period={date}|process=data cleaning|status=succeeded")

def load_data(date):
    config, _ = get_dw_config()
    gp_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['dbname']}"
    properties = {
        'user':config['user'],
        'password':config['password'],
        'driver':'org.postgresql.Driver',
        'batch_size':'1000'
    }
    spark = SparkSession\
                .builder\
                .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
                .appName("loading_out_of_stock_data")\
                .getOrCreate()
    data = spark.read.parquet(f'{SILVER_DIR}/{API_NAME}/date={date}')
    data\
        .withColumn('date', F.lit(date))\
        .withColumn('date', F.col('date').cast(DateType()))\
        .write.jdbc(gp_url, f"{config['schema']}.out_of_stock", properties=properties, mode='append')

with DAG(
    dag_id = 'out_of_stock_data_collection',
    start_date = datetime(2021, 1, 1),
    end_date = datetime(2021, 1, 3),
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
    
    get_data_task = PythonOperator(
        task_id = 'load_to_bronze_out_of_stock_data',
        python_callable=get_data,
        op_kwargs={'date':'{{ds}}'},
        trigger_rule='all_success'
    )
    clean_data_task = PythonOperator(
        task_id = 'load_to_silver_out_of_stock_data',
        python_callable=clean_data,
        op_kwargs={'date':'{{ds}}'},
        trigger_rule='all_success'
    )
    load_data_task = PythonOperator(
        task_id = 'load_to_gold_out_of_stock_data',
        python_callable=load_data,
        op_kwargs={'date':'{{ds}}'},
        trigger_rule='all_success'
    )
    get_data_task>>clean_data_task>>load_data_task