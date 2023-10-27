import json
import configparser
from datetime import datetime
import logging
from extract_manufacturer_communications import extract
from transform_manufacturer_communications import transform
from load_manufacturer_communications import load

config = configparser.ConfigParser()
config.read('NHTSA\ManufacturerCommunications\config\.env')

date_time= datetime.now().strftime("%Y-%m-%d")

def manufacturerCommunications():

    extract(config)

    df = transform()

    load(df,'NHTSA_ManufacturerCommunications','NHTSA.db')

if __name__ == '__main__':
    manufacturerCommunications()
    