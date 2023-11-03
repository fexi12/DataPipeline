import json
import configparser
from datetime import datetime
import logging
from extract_complaints import extract
from transform_complaints import transform
from load_complaints import load
import pathlib

config_path = pathlib.Path(__file__).parent.absolute() / "config/.env"
config = configparser.ConfigParser()
config.read(config_path)



def complaints():

   

    #df = transform(config)

    #load(df,'NHTSA_Complaints','NHTSA.db')

    return extract(config)

if __name__ == '__main__':
    complaints()
    