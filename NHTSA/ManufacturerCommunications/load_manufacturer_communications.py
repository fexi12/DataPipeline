import pandas as pd
import sqlite3

def load(df:pd.DataFrame,table,DB) -> None:
        engine = sqlite3.connect(DB)
        df.to_sql(table,engine, if_exists='replace')
        #df.show()