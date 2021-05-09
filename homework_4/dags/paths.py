import os
os.chdir('/home/user/airflow/dags')

OUT_OF_STOCK_DATA_PATH = os.path.join(os.getcwd(), 'data', 'out_of_stock')
DBSHOT_DATA_PATH = os.path.join(os.getcwd(), 'data', 'dbshop')
for path in (OUT_OF_STOCK_DATA_PATH, DBSHOT_DATA_PATH):
    if not os.path.exists(path):
        os.mkdir(path)
CONFIG_PATH = os.path.join(os.getcwd(), 'api_config.yml')
DB_CONFIG_PATH = os.path.join(os.getcwd(), 'db_config.yml')