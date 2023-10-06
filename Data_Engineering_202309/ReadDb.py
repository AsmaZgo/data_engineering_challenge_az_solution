import sqlite3
import pandas as pd


class ReadDb:
    def __init__(self):
        pass

    def query_data_from_database(self, database_path):
        conn = sqlite3.connect(database_path)
        query = "SELECT * FROM session_sources"
        data = pd.read_sql_query(query, conn)
        conn.close()
        return data

    def query_all_data_from_database(self, database_path):
        conn = sqlite3.connect(database_path)

        session_sources_query = "SELECT * FROM session_sources"
        conversions_query = "SELECT * FROM conversions"
        session_costs_query = "SELECT * FROM session_costs"

        session_sources_data = pd.read_sql_query(session_sources_query, conn)
        conversions_data = pd.read_sql_query(conversions_query, conn)
        session_costs_data = pd.read_sql_query(session_costs_query, conn)

        conn.close()
        return session_sources_data, conversions_data, session_costs_data


