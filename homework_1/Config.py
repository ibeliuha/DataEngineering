import os
os.chdir('C:\\Users\\ibeli\\OneDrive\\Documents\\DataEngineering\\homework_1')  #Please, change the dirrectory which is used in your machine
from PATHS import API_CONFIG_PATH, LOG_PATH
import requests
import logging
import yaml
import json
from datetime import datetime
from functools import reduce
logging.basicConfig(filename=LOG_PATH, level=logging.WARNING)
loger = logging.getLogger()


class Config():
    """
    Configuration object which incapsulate method to work with API configurations written in config.yml
    """

    def __init__(self, api_name):
        """
        Parameters:
        api_name (str): api_name (str): name of API in config.yml file
        """
        with open(API_CONFIG_PATH, 'r') as conf_file:
            try:
                conf = yaml.load(conf_file, Loader=yaml.FullLoader)[api_name]
                self.api_name = api_name
                self.url = conf.get('base_url',None)
                self.auth = conf.get('auth',None)
                self.endpoints = conf.get('endpoints',None)
                self.add_params = reduce(lambda x, y: dict(**x, **y), conf.get('http_params', None))
            except KeyError:
                logging.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=ApiNameError|description=No such API exists in {API_CONFIG_PATH}")
    def get_access_token(self):
        try:
            token = requests.post(
                url=self.url+self.auth['auth_endpoint'],
                data=json.dumps(dict(filter(lambda x: x[0] !='auth_endpoint', self.auth.items()))),
                #data=json.dumps(dict(zip(['username','password'],[self.user, self.password]))), 
                **self.add_params
                ).json()['access_token']
        except requests.HTTPError as e:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|description={e}|ApiName={self.api_name}")
            return None
        except KeyError:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|description=No token returned|ApiName={self.api_name}")
            return None
        except Exception as e:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|description={e}|ApiName={self.api_name}")
            return None
        return f"JWT {token}"