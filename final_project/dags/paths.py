import os
import hdfs
os.chdir('/home/user/airflow/dags')

CONFIG_PATH = os.path.join(os.getcwd(), 'config.yml')
#hdfs directories
BRONZE_DIR = '/bronze'
SILVER_DIR = '/silver'
GOLD_DIR = '/gold'