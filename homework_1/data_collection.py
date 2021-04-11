import os
os.chdir('C:\\Users\\ibeli\\OneDrive\\Documents\\DataEngineering\\homework_1')  #Please, change the dirrectory which is used in your machine
import errno
import requests
import logging
import yaml
import json
from datetime import datetime
from itertools import product
from time import sleep
import pandas as pd

from PATHS import REPORT_PATH, LOG_PATH
from Config import Config

logging.basicConfig(filename=LOG_PATH, level=logging.WARNING)
loger = logging.getLogger()

def get_data(api_name, endpoint, params=None, split_by_param=0, output_file_format='json'):
    """
    Summary:
    function collect data and write it to the disk
    
    Parameters:
    api_name (str): name of API in config.yml file
    endpoint (str): endpoint from which data must be collected
    params (OrderedDict): dictionary of parameters which must be passed to endpoint in order to get data. Please, note that parameters values must be passed in list (default=None)
    split_by_param (int): order of parameter in param variable by which data is supposed to be splitted in different folders (default=0)
    output_file_format (str): file format in which data must be saved to the disk.  (default='json')
    """
    conf = Config(api_name=api_name)
    conf.add_params['headers']['Authorization'] = conf.get_access_token()
    try:
        endpoint = conf.endpoints[endpoint]
        uri = endpoint.get('uri',None)
        not_allowable_params = [x for x in params if x not in endpoint['params']]
        if len(not_allowable_params)==0:
            all_comb = product(*list(params.values()))
            for comb in all_comb:
                result = requests.get(
                                url=conf.url+uri,
                                data=json.dumps(dict(zip(params.keys(), comb))),
                                **conf.add_params
                            ).text
                write_data(data=result, folder_name=comb[0], file_name='+'.join(comb), file_format=output_file_format)
                sleep(1)
        else:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|description=Passed not allowable params - {','.join(not_allowable_params)}|ApiName={api_name}")
    except KeyError:
        loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|description=No endpoint with name '{endpoint}' exists|ApiName={api_name}")
    except requests.HTTPError as e:
        loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=APIError|error={e}|ApiName={api_name}")

def write_data(data, folder_name, file_name, file_format='json'):
    directory = os.path.join(REPORT_PATH, folder_name)
    try:
        os.mkdir(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=IOError|description=directory {directory} can not be created|error={e}")
        else:
            pass
    with open(os.path.join(directory, f'{file_name}.{file_format}'), 'w') as f:
        try:
            if file_format=='json':
                d = {}
                d['data'] = json.loads(data)
                json.dump(d, f)
            if file_format=='csv':
                pd.read_json(data).to_csv(f)
        except IOError as e:
            loger.error(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|type=IOError|description=file can not be written to folder {folder_name}|error={e}")