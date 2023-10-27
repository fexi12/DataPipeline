
import logging
import configparser
from datetime import datetime
from transform_investigations import transform
from load_investigations import load
from extract_investigations import extract


config = configparser.ConfigParser()
config.read('\\NHTSA\\Investigations\\config\\.env')
date_time= datetime.now().strftime("%Y-%m-%d")

path_in = config['FILES']['IN']

def investigations():
    
    extract()

    df = transform()
    
    load(df,'NHTSA_investigations','NHTSA.db')

if __name__ == '__main__':
    investigations()