# Description: 
Final project for Data Engineering course. 
# Note:
- Since it's daily ETL data from orders and out_of_stock are extracted for the DAG's execution date and loaded to parquet and DWH in append mode.
- Please, make sure to execute DW.sql script first in order to create tables needed.
- All connections are stored as AirflowConnection and their ids are specified in config.yml file.
