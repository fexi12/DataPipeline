#correctNull
import logging
import sqlite3

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

def correctNull(df):
    df_aux = df.fillna('')
    #df['VehiclePicture'].fillna(' ', inplace=True)
    #df['FrontCrashPicture'].fillna(' ', inplace=True)
    #df['SideCrashPicture'].fillna(' ', inplace=True)
    #df['SideCrashVideo'].fillna(' ', inplace=True)
    #df['FrontCrashVideo'].fillna(' ', inplace=True)
    #df[]SidePolePicture SidePoleVideo
    
    if(df_aux.isnull().sum().sum()):
        columns_with_null = df.columns[df.isnull().any()]
        logging.debug(df[df.isnull().any(axis=1)][columns_with_null].head())
        logging.error(f"Please treat the null values in {columns_with_null.to_numpy()} with id")
    
    return df_aux



def load(df:pandas.DataFrame,table,DB) -> None:
    engine = sqlite3.connect(DB)
    df.to_sql(table,engine, if_exists='replace')