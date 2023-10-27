from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
from pyspark.sql.functions import count,when,col,exists
import configparser
from datetime import datetime
#from load_investigations import load
import json

config = configparser.ConfigParser()
config.read('NHTSA\Recalls\config\.env')

path_in = config['FILES']['IN']
path_out = config['FILES']['OUT']
file_JSON = config['NAME']['JSON']
file_xlsx = config['NAME']['XLSX']

date_time= datetime.now().strftime("%Y-%m-%d")
path_date_filename_in = path_in + '\\' + date_time + '-' + file_JSON
path_date_filename_out = path_out + '\\' + date_time + '-' + file_xlsx


def transform(file:json):
  
    spark = SparkSession.builder.getOrCreate()

    #py_df = spark.read.json(path_date_filename_in, schema=schema)
    
    py_df = spark.read.json(path_date_filename_in)
    #Debug purpose
    #col_null_cnt_df =  py_df.select([count(when(col(c).isNull(),c)).alias(c) for c in py_df.columns])
    #col_null_cnt_df.show()
    #py_df.filter(py_df.COMPNAME.isNull() & py_df.MFR_NAME.isNull()).show()

    py_df = py_df.fillna(value="")
    df = py_df.toPandas()


    #TODO use the same data format "%Y-%m-%d"

    #TODO , problem with data 
    df.to_csv(path_date_filename_out,index=False) 


    return df


''' schema = StructType([
        StructField("MFGTXT", StringType(), False),
        StructField("CAMPNO", StringType(), False),
        StructField("MFGCAMPNO", StringType(), False),
        StructField("RCLSUBJ", StringType(), False),
        StructField("ODATE", StringType(), False),
        StructField("ODATEEND", StringType(), False),
        StructField("RPTNO", IntegerType(), False),
        StructField("REPORT_YEAR", StringType(), False),
        StructField("INVOLVED", IntegerType(), False),
        StructField("TTLREMEDIED", IntegerType(), False),
        StructField("TTLUNREACH", IntegerType(), False),
        StructField("TTLREMOVED", IntegerType(), False),
        StructField("SUBMDATE", StringType(), False)
])
'''