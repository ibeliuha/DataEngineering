import os
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import requests
import yaml
import json
from datetime import datetime
from paths import CONFIG_PATH, LOG_PATH
import logging
#some piece of code which resolve the problem when loger doesn't write anything to a file
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename=LOG_PATH, level=logging.WARN)


class Config():
    """
    Configuration object which incapsulate method to work with API configurations written in config.yml
    """
    def __init__(self):
        with open(CONFIG_PATH, 'r') as conf_file:
            is_read = True
            try:
                conf = yaml.load(conf_file, Loader=yaml.FullLoader)['app']['API']
                conn = BaseHook.get_connection(conf['airflow_conn_id'])
            except KeyError:
                logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=ApiNameError|error=No such API exists in {API_CONFIG_PATH}")
                is_read = False
            except AirflowExceptions as e:
                logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=ConnectionIdError|error={e}")
                is_read = False
            if is_read:
                self.auth_url = conn.host+conf['authorization']['endpoint']
                self.auth_data = {'username':conn.login, 'password':conn.password}
                self.endpoint_url = conn.host+conf['data']['endpoint']
                self.headers = conf.get('headers', dict())
                self.headers['Authorization']=self._get_access_token()

    def _get_access_token(self, token_type='JWT'):
        self.headers.pop('Authorization')
        try:
            result = requests.post(
                url=self.auth_url,
                data=json.dumps(self.auth_data),
                headers=self.headers
                )
            result.raise_for_status()
        except Exception as e:
            logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=TokenError|error={e}")
        return ' '.join([token_type, result.json()['access_token']])