import json
import configparser
from datetime import datetime
import logging
from extract_complaints import extract
from transform_complaints import transform
from load_complaints import load
import pathlib

config_path = pathlib.Path(__file__).parent.absolute() / "config/config.ini"
config = configparser.ConfigParser()
config.read(config_path)



def complaints():

    extract(config)

    df = transform(config)

    load(df,'NHTSA_Complaints','NHTSA.db')

if __name__ == '__main__':
    complaints()
    