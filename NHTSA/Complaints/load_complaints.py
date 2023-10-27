import pandas as pd
import sqlalchemy as sa
import sqlite3
import pyodbc
import urllib
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from sqlalchemy import create_engine

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
#conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=(local);PORT=1433;DATABASE=NHTSA;Trusted_Connection=yes')
conn = sa.create_engine("mssql+pyodbc://(local)/NHTSA?driver=ODBC Driver 17 for SQL Server?trusted_connection=yes")

#"mssql+pyodbc:///?odbc_connect={};Trusted_Connection=yes".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format('(local)', 'NHTSA', user, password)))
#engine = sa.create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{db}?driver=SQL+Server?Trusted_Connection=yes')
def load(df:pd.DataFrame,table,DB) -> None:
        #engine = sqlite3.connect(DB)
        df.to_sql(table, conn, if_exists='append')
        #df.show()



def load_to_azure():
        connection_url = sa.engine.URL.create(
        "mssql+pyodbc",
        username="",
        password="",
        host="dw.azure.example.com",
        database="mydb",
        query={
        "driver": "ODBC Driver 17 for SQL Server",
        "autocommit": "True",
        },
        )

        engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT"
        )