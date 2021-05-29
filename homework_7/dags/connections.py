import yaml
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook
from paths import CONFIG_PATH

def get_hdfs_config(conf_id='HDFS', conf_type='connection'):
    if conf_type=='connection':
        with open(CONFIG_PATH, 'r') as conf_file:
            conn_id = yaml.load(conf_file, Loader=yaml.FullLoader)['app'][conf_id]['airflow_conn_id']
            conn = BaseHook.get_connection(conn_id)
            url = conn.host+':'+str(conn.port)+'/'
            user = conn.login
        return url, user
    elif conf_type=='partitions':
        with open(CONFIG_PATH, 'r') as conf_file:
             return yaml.load(conf_file, Loader=yaml.FullLoader)['app'][conf_id]['partitions']



def get_db_config(dbname, conf_id='DB'):
    with open(CONFIG_PATH, 'r') as conf_file:
        conn_id = yaml.load(conf_file, Loader=yaml.FullLoader)['app'][conf_id]['airflow_conn_id']
        conn = BaseHook.get_connection(conn_id)
    
        conf = dict()
        conf['user'] = conn.login
        conf['password'] = conn.password
        conf['host'] = conn.host
        conf['port'] = conn.port
        conf['dbname'] = dbname
    return conf