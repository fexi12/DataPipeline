import json
from extract_recalls import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import configparser
from datetime import datetime
from transform_recalls import transform
from load_recalls import load


config = configparser.ConfigParser()
config.read('NHTSA\Recalls\config\.env')

date_time= datetime.now().strftime("%Y-%m-%d")

path_in = config['FILES']['IN']

def recalls():
    
    

    modelYear = getModelYears()
    
    make = getMakes(modelYear)

    model = getModels(make)

    recalls = getRecalls(model)

    path = path_in + '\\' + date_time + '-' + 'recalls.json'

    with open(path, 'w') as f:
        json.dump(recalls, f)

    df = transform(recalls)
    
    load(df,'NHTSA_Recalls','NHTSA.db')

if __name__ == '__main__':
    recalls()