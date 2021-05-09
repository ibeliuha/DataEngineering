import os
import requests
import yaml
import json
from datetime import datetime
from paths import CONFIG_PATH

class Config():
    """
    Configuration object which incapsulate method to work with API configurations written in config.yml
    """
    def __init__(self):
        with open(CONFIG_PATH, 'r') as conf_file:
            try:
                conf = yaml.load(conf_file, Loader=yaml.FullLoader)['app']
            except KeyError:
                logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=ApiNameError|description=No such API exists in {API_CONFIG_PATH}")
            self.auth_url = conf['base_url']+conf['auth'].pop('endpoint')
            self.auth_data = conf['auth']
            self.endpoint_url = conf['base_url']+conf['data']['endpoint']
            self.headers = conf.get('headers', dict())
            self.headers['Authorization']=self._get_access_token()

    def _get_access_token(self, token_type='JWT'):
        
        self.headers.pop('Authorization')
        result = requests.post(
            url=self.auth_url,
            data=json.dumps(self.auth_data),
            headers=self.headers
            )
        result.raise_for_status()
        return ' '.join([token_type, result.json()['access_token']])