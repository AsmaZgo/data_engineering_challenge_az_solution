import sqlite3
import pandas as pd


class ReadDb:
    def __init__(self,database_path):
        self.conn = sqlite3.connect(database_path)

    def open_connection(self,database_path):
        self.conn = sqlite3.connect(database_path)
    def getFilteredSessions(self, start, end):
        query = "SELECT * FROM session_sources WHERE event_date BETWEEN ? and ?"
        data = pd.read_sql_query(query, self.conn, params=(start, end))
        return data

    def getSessions(self):
        query = "SELECT * FROM session_sources"
        data = pd.read_sql_query(query, self.conn)
        return data

    def query_data_from_database(self, table_name):

        query = "SELECT * FROM {}".format(table_name)
        data = pd.read_sql_query(query, self.conn)

        return data

    def query_all_data_from_database(self):

        session_sources_query = "SELECT * FROM session_sources"
        conversions_query = "SELECT * FROM conversions"
        session_costs_query = "SELECT * FROM session_costs"

        session_sources_data = pd.read_sql_query(session_sources_query, self.conn)
        conversions_data = pd.read_sql_query(conversions_query, self.conn)
        session_costs_data = pd.read_sql_query(session_costs_query, self.conn)

        return session_sources_data, conversions_data, session_costs_data

    def close_connection(self):
        self.conn.close()