PROJECT_NAME: Hometask1_08.4.2021
CREATOR: Ihor Beliuha
DESCRIPTION:
Hometask from the second lesson of Data Engineering course.
Project are supposed to be used for data collection from different API's

STRUCTURE OF PROJECT:
file Config provide a Config object in order to provide incapsulated methods to work with API configuration data in config.yml
module data_collections contains functions get_data and write_data which are self-desctibed
module PATHS contains constants in which paths to different config files or folders are stored
folder data are created for storing all information collected from API (currently json and csv formats are available)
