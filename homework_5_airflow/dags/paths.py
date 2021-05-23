import os
import hdfs
os.chdir('/home/user/airflow/dags')

OUT_OF_STOCK_DATA_PATH = os.path.join(os.getcwd(), 'bronze_layer', 'out_of_stock')
if not os.path.exists(OUT_OF_STOCK_DATA_PATH):
    os.mkdir(OUT_OF_STOCK_DATA_PATH)
CONFIG_PATH = os.path.join(os.getcwd(), 'config.yml')
LOG_PATH = os.path.join(os.getcwd(), 'log.dat')