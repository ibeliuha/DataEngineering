import os
os.chdir('C:\\Users\\ibeli\\OneDrive\\Documents\\DataEngineering\\homework_1') #Please, change the dirrectory which is used in your machine
from collections import OrderedDict
from data_collection import get_data

if __name__ == '__main__':
    params = OrderedDict()
    params['date'] = ['2021-01-01','2021-01-02','2021-01-03']
    get_data(
        api_name='robot_dreams',
        endpoint='out_of_stock',
        params=params,
        split_by_param=0,
        output_file_format='csv'
        )