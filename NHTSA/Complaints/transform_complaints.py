from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
from pyspark.sql.functions import count,when,col,exists
import configparser
from datetime import datetime
from load_complaints import load
import json


date_time= datetime.now().strftime("%Y-%m-%d")


def transform(config:configparser):
  
    path_in = config['FILES']['IN']
    path_out = config['FILES']['OUT']
    file_txt = config['NAME']['ZIP']
    file_xlsx = config['EXCELL']['XLXS']

    path_date_filename_in = path_in + '\\' + date_time + '-' + file_txt
    path_date_filename_out = path_out + '\\' + date_time + '-' + file_xlsx


    spark = SparkSession.builder.config("spark.executor.memory", '4G') \
    .config("spark.driver.memory", '10G').getOrCreate()


    #Based on CMPL.txt
    schema = StructType([
        StructField("CMPLID", StringType(), True),
        StructField("ODINO", StringType(), True),
        StructField("MFR_NAME", StringType(), True),
        StructField("MAKETXT", StringType(), True),
        StructField("MODELTXT", StringType(), True),
        StructField("YEARTXT", StringType(), True),
        StructField("CRASH", StringType(), True),
        StructField("FAILDATE", StringType(), True),
        StructField("FIRE", StringType(), True),
        StructField("INJURED", IntegerType(), True),
        StructField("DEATHS", IntegerType(), True),
        StructField("COMPDESC", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("VIN", StringType(), True),
        StructField("DATEA", StringType(), True),
        StructField("LDATE", StringType(), True),
        StructField("MILES", IntegerType(), True),
        StructField("OCCURENCES", IntegerType(), True),
        StructField("CDESCR", StringType(), True),
        StructField("CMPL_TYPE", StringType(), True),
        StructField("POLICE_RPT_YN", StringType(), True),
        StructField("PURCH_DT", StringType(), True),
        StructField("ORIG_OWNER_YN", StringType(), True),
        StructField("ANTI_BRAKES_YN", StringType(), True),
        StructField("CRUISE_CONT_YN", StringType(), True),
        StructField("NUM_CYLS", IntegerType(), True),
        StructField("DRIVE_TRAIN", StringType(), True),
        StructField("FUEL_SYS", StringType(), True),
        StructField("FUEL_TYPE", StringType(), True),
        StructField("TRANS_TYPE", StringType(), True),
        StructField("VEH_SPEED", IntegerType(), True),
        StructField("DOT", StringType(), True),
        StructField("TIRE_SIZE", StringType(), True),
        StructField("LOC_OF_TIRE", StringType(), True),
        StructField("TIRE_FAIL_TYPE", StringType(), True),
        StructField("ORIG_EQUIP_YN", StringType(), True),
        StructField("MANUF_DT", StringType(), True),
        StructField("SEAT_TYPE", StringType(), True),
        StructField("RESTRAINT_TYPE", StringType(), True),
        StructField("DEALER_NAME", StringType(), True),
        StructField("DEALER_TEL", StringType(), True),
        StructField("DEALER_CITY", StringType(), True),
        StructField("DEALER_STATE", StringType(), True),
        StructField("DEALER_ZIP", StringType(), True),
        StructField("PROD_TYPE", StringType(), True),
        StructField("REPAIRED_YN", StringType(), True),
        StructField("MEDICAL_ATTN", StringType(), True),
        StructField("VEHICLES_TOWED_YN", StringType(), True)
])

    

    py_df = spark.read.option("delimiter","\t").csv(path_date_filename_in, header=False, schema=schema)

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
    df.set_index('CMPLID',inplace=True,verify_integrity=True)


    df['MANUF_DT'] = pd.to_datetime(df['MANUF_DT'].astype(str), format='%Y-%m-%d',errors = 'coerce')
    df['PURCH_DT'] = pd.to_datetime(df['PURCH_DT'].astype(str), format='%Y-%m-%d',errors = 'coerce')
    df['LDATE'] = pd.to_datetime(df['LDATE'].astype(str), format='%Y-%m-%d',errors = 'coerce')
    df['DATEA'] = pd.to_datetime(df['DATEA'].astype(str), format='%Y-%m-%d',errors = 'coerce')
    

    df.to_csv(path_date_filename_out,index=False) 

    load(df,'NHTSA_Investigations','NHTSA.db')

    return df



