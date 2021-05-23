import os
import hdfs
os.chdir('/home/user/airflow/dags')

CONFIG_PATH = os.path.join(os.getcwd(), 'config.yml')
if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
    os.mkdir(os.path.join(os.getcwd(), 'logs'))
LOG_PATH = os.path.join(os.getcwd(), 'logs')

#hdfs directories
BRONZE_DIR = 'bronze/'
SILVER_DIR = 'silver/'
GOLD_DIR = 'gold/'