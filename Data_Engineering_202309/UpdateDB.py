import sqlite3
import pandas as pd


class UpdateDb:
    def __init__(self):
        pass
    def WriteDFtoDB(self,database_path,df):
        conn = sqlite3.connect(database_path)
        df.to_sql('attribution_results', conn, index=True, if_exists='replace')
        conn.close()