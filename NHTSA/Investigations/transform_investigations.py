from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
from pyspark.sql.functions import count,when,col,exists
import configparser
import dateutil
from datetime import datetime, date
from load_investigations import load

config = configparser.ConfigParser()
config.read('NHTSA\Investigations\config\.env')

path_in = config['FILES']['IN']
path_out = config['FILES']['OUT']
file_txt = config['NAME']['TXT']
file_zip = config['NAME']['ZIP']
file_csv = config['EXCELL']['CSV']

date_time= datetime.now().strftime("%Y-%m-%d")
zip_file_name_in = path_in + '\\' + date_time + '-' + file_zip
zip_file_name_out = path_out + '\\' + date_time + '-' + file_csv


def transform_investigations():

    transform()


def transform():
  
    spark = SparkSession.builder.getOrCreate()


    schema = StructType([
        StructField("NHTSA_ACTION_NUMBER", StringType(), True),
        StructField("MAKE", StringType(), True),
        StructField("MODEL", StringType(), True),
        StructField("YEAR", StringType(), True),
        StructField("COMPNAME", StringType(), True),
        StructField("MFR_NAME", StringType(), True),
        StructField("ODATE", StringType(), True),
        StructField("CDATE", StringType(), True),
        StructField("CAMPNO", StringType(), True),
        StructField("SUBJECT", StringType(), True),
        StructField("SUMMARY", StringType(), True),
        StructField("SUMMARY 0.5", StringType(),True)
    ])

    py_df = spark.read.option("delimiter","\t").csv(zip_file_name_in
, schema=schema, header=False)

    #Debug purpose
    #col_null_cnt_df =  py_df.select([count(when(col(c).isNull(),c)).alias(c) for c in py_df.columns])
    #col_null_cnt_df.show()
    #py_df.filter(py_df.COMPNAME.isNull() & py_df.MFR_NAME.isNull()).show()

    py_df = py_df.fillna(value="")
    df = py_df.toPandas()

    #Debug purpose
    #col_null_cnt_df =  py_df.select([count(when(col(c).isNull(),c)).alias(c) for c in py_df.columns])
    #col_null_cnt_df.show()
 
    #TODO Check index 
    #df.set_index('CAMPNO',inplace=True)
    #df.set_index('CAMPNO',inplace=True,verify_integrity=True)


    df['ODATE'] = pd.to_datetime(df['ODATE'].astype(str), format='%Y-%m-%d')
    df['CDATE'] = pd.to_datetime(df['CDATE'].astype(str), format='%Y-%m-%d')


    #TODO , problem with data 
    df.to_csv(zip_file_name_out,index=False) 

    return df

transform_investigations()