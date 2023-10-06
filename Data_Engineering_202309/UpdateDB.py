import sqlite3
import pandas as pd


class UpdateDb:
    def __init__(self):
        pass
    def WriteDFtoDB(self,database_path,df):
        conn = sqlite3.connect(database_path)
        df.to_sql('attribution_customer_journey', conn, index=False, if_exists='replace')
        conn.close()