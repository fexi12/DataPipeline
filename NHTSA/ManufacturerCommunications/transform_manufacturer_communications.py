from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
from pyspark.sql.functions import count,when,col,exists
import configparser
from datetime import datetime, date
from load_manufacturer_communications import load

config = configparser.ConfigParser()
config.read('NHTSA\ManufacturerCommunications\config\.env')

path_in = config['FILES']['IN']
path_out = config['FILES']['OUT']
file_txt = config['NAME']['TXT']
file_zip = config['NAME']['ZIP']
file_csv = config['EXCELL']['CSV']

date_time= datetime.now().strftime("%Y-%m-%d")
zip_file_name_in = path_in + '\\' + date_time + '-' + file_zip
zip_file_name_out = path_out + '\\' + date_time + '-' + file_csv


def transform():

    transform_json()


def transform_json():
  
    spark = SparkSession.builder.getOrCreate()


    schema = StructType([
        StructField("BULNO", StringType(), False),
        StructField("BULREP", StringType(), False),
        StructField("ID", IntegerType(), False),
        StructField("BULDTE", StringType(), False),
        StructField("COMPNAME", StringType(), False),
        StructField("MAKETXT", StringType(), False),
        StructField("MODELTXT", StringType(), False),
        StructField("YEARTXT", StringType(), False),
        StructField("DATEA", StringType(), False),
        StructField("SUMMARY", StringType(), False)
    ])

    py_df = spark.read.option("delimiter","\t").csv(zip_file_name_in
, schema=schema, header=False)


    py_df = py_df.fillna(value="")
    df = py_df.toPandas()

    #TODO Check index 
    #df.set_index('CAMPNO',inplace=True)
    #df.set_index('CAMPNO',inplace=True,verify_integrity=True)

    #TODO use the same data format "%Y-%m-%d"

    df['ODATE'] = pd.to_datetime(df['ODATE'].astype(str), format='%Y-%m-%d')
    df['CDATE'] = pd.to_datetime(df['CDATE'].astype(str), format='%Y-%m-%d')

    #TODO , problem with data 
    df.to_csv(zip_file_name_out,index=False) 

    return df
