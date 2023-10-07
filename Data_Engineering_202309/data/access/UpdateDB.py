import sqlite3
import pandas as pd


class UpdateDb:
    def __init__(self):
        pass
    def connect(self, conn):
        self.conn = conn

    def openConn(self,database_path):
        self.conn = sqlite3.connect(database_path)
    def WriteDFtoDB(self,database_path,df,table_name):
        conn = sqlite3.connect(database_path)
        df.to_sql(table_name, conn, index=False, if_exists='replace')
        conn.close()

    def WriteDFtoDBFromConn(self,df,table_name):
        df.to_sql(table_name, self.conn, index=False, if_exists='replace')


    def close_connection(self):
        self.conn.close()