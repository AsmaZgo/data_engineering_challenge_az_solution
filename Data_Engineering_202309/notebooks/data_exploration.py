import sqlite3
import pandas as pd
if __name__ == '__main__':
    database_path="/Users/zgolli/PycharmProjects/challenge.db"

    conn = sqlite3.connect(database_path)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    #query = "SELECT * from conversions"

    data = pd.read_sql_query(query, conn)
    print(data)


    # Understanding the conversions table
    query = "SELECT * from conversions"
    data = pd.read_sql_query(query, conn)
    print(data.T)

    # checking the attribution results
    query = "SELECT * from attribution_results"
    data = pd.read_sql_query(query, conn)
    print(data.T)
    conn.close()