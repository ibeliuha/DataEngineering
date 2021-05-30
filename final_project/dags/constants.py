import os
import hdfs
os.chdir('/home/user/airflow/dags')

DBNAME='dshop_bu'
DW_DBNAME='postgres'
CONFIG_PATH = os.path.join(os.getcwd(), 'config.yml')
#hdfs directories
BRONZE_DIR = '/bronze'
SILVER_DIR = '/silver'
GOLD_DIR = '/gold'