import os
os.chdir('/home/user/airflow/dags')
import psycopg2
import hdfs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import DummyOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from connections import get_db_config, get_hdfs_config, get_dw_config
import yaml
from paths import BRONZE_DIR,SILVER_DIR
import logging

#define database name
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
        tables = [x[0] for x in tables]
    return tables

def get_data(as_of_date, table, config):
    #defining key, value pair for sql queries which is conditionaly extracted. This approach is probably better then if else statements
    conditional_sql = {
        "orders": f"COPY (SELECT * FROM public.orders WHERE order_date='{as_of_date}') TO STDOUT WITH HEADER CSV"
    }
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data extraction|status=started")
    directory = f'{BRONZE_DIR}/{DBNAME}/{table}'
    os.system(f'hadoop fs -mkdir -p {directory}') #create directory if not exists
    try:
        with psycopg2.connect(**config) as con:
            cursor = con.cursor()
            #hdfs connection
            url, user = get_hdfs_config(conf_type='connection')
            client = hdfs.InsecureClient(url, user=user)
            with client.write(f"{directory}/{as_of_date}.csv") as output:
                cursor.copy_expert(conditional_sql.get(table, f'COPY public.{table} TO STDOUT WITH HEADER CSV'), output)
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
        directory = f'{SILVER_DIR}/{DBNAME}/{table}'
        os.system(f'hadoop fs -mkdir -p {directory}') #create directory if not exists
        try:
            write_mode = get_hdfs_config(conf_type='write_modes')[table]
        except KeyError:
            logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=failed|error=undefined write_mode for given table")
            raise AirflowException()
        try:
            partition = get_hdfs_config(conf_type='partitions')[table]
            data.write\
                .partitionBy(partition)\
                .parquet(f'{directory}', mode=write_mode)
        except KeyError:
            data.write\
                .parquet(f'{directory}', mode=write_mode)
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=failed|error={e}")
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=data cleaning|status=succeeded")

def load_data(as_of_date, table, config):
    gp_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['dbname']}"
    properties = {
        'user':config['user'],
        'password':config['password'],
        'driver':'org.postgresql.Driver',
        'batch_size':'5000'
    }
    def load_orders(spark):
        orders = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/orders/order_date={as_of_date}')
        orders\
            .groupBy("product_id","client_id", "store_id")\
            .agg(F.sum("quantity").alias("quantity"))\
            .withColumn("order_date", F.lit(as_of_date))\
            .withColumn("order_date", F.col("order_date").cast(DateType()))\
            .write.jdbc(gp_url, f"{config['schema']}.orders", properties=properties, mode='append')
    def load_products(spark):
        products = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/products')
        departments = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/departments')
        aisles = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/aisles')
        products\
            .join(departments, on=['department_id'], how='left')\
            .join(aisles, on=['aisle_id'], how='left')\
            .select('product_id','product_name', 'department','aisle')\
            .write.jdbc(gp_url, f"{config['schema']}.dim_products", properties=properties, mode='overwrite')
    def load_stores(spark):
        stores = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/stores')
        store_types = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/store_types')
        location_areas = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/location_areas')
        stores\
            .join(store_types, on=['store_type_id'], how='left')\
            .join(location_areas, on=stores['location_area_id']==location_areas['area_id'], how='left')\
            .select('store_id', 'type', 'area')\
            .write.jdbc(gp_url, f"{config['schema']}.dim_stores", properties=properties, mode='overwrite')
    def load_clients(spark):
        clients = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/clients')
        location_areas = spark.read.parquet(f'{SILVER_DIR}/{DBNAME}/location_areas')
        clients\
            .join(location_areas, on=clients['location_area_id']==location_areas['area_id'], how='left')\
            .select('id', 'fullname', 'area')\
            .write.jdbc(gp_url, f"{config['schema']}.dim_clients", properties=properties, mode='overwrite')

    #mapping tables to function
    func_dict = {
        'orders': load_orders,
        'dim_clients': load_clients,
        'dim_stores': load_stores,
        'dim_products': load_products
        }
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=loading data to DW|status=started")
    try:
        spark = SparkSession\
                    .builder\
                    .config('spark.jars', '/home/user/shared_folder/postgresql-42.2.20.jar')\
                    .appName("loading_dshop_data_to_DW")\
                    .getOrCreate()
        func_dict[table](spark=spark)
    except KeyError:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=loading data to DW|status=failed|error=no function for table {table} defined")
        raise AirflowException()
    except Exception as e:
        logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=loading data to DW|status=failed|error={e}")
        raise AirflowException()
    logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|table={table}|process=loading data to DW|status=succeeded")

with DAG(
    dag_id = 'dbshop_data_collection',
    start_date = datetime(2021,1,1),
    end_date = datetime(2021,1,3),
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
    gold_dummy = DummyOperator(
        task_id = 'gold_layer',
        dag=dag,
        trigger_rule='none_skipped'
    )

    #BRONZE&SILVER TASKS
    config_db = get_db_config(dbname=DBNAME)
    tables_db = get_all_tables(config_file=config_db)
    bronze_tasks = []
    silver_tasks = []
    for table in tables_db:
        bronze_tasks.append(
            PythonOperator(
                task_id = table+'_bronze',
                python_callable=get_data,
                op_kwargs={
                    'as_of_date':'{{ds}}',
                    'table':table,
                    'config':config_db
                },
                dag=dag,
                trigger_rule='all_success'
            )
        )
        silver_tasks.append(
            PythonOperator(
                task_id = table+'_silver',
                python_callable=clean_data,
                op_kwargs={
                    'as_of_date':'{{ds}}',
                    'table':table
                },
                dag=dag,
                trigger_rule='all_success'
            )
        )
    #GOLD TASKS
    config_dw, tables_dw = get_dw_config()
    gold_tasks = []
    for table in tables_dw['DB']:
        gold_tasks.append(
            PythonOperator(
                task_id = table+'_gold',
                python_callable=load_data,
                op_kwargs={
                    'as_of_date': '{{ds}}',
                    'table':table,
                    'config':config_dw
                },
                dag=dag,
                trigger_rule='all_success'
            )
        )
    bronze_tasks>>bronze_dummy>>silver_tasks>>silver_dummy>>gold_tasks>>gold_dummy

    
    

