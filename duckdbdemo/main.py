import os
import duckdb
import pyodbc
import pandas as pd

def main():
    print('hello')
    print('world')

    server = 'DESKTOP-AN9FM1B\SQLEXPRESS'  # Example: 'localhost' or 'your_server.database.windows.net'
    database = 'AdventureWorks2022'
    username = 'sa'
    password = '02021988sp'  

    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    try:
        conn = pyodbc.connect(conn_str)

        query = "select * from [Person].[Person]"

        df = pd.read_sql(query, conn)

        #print(df)

        duck_conn =duckdb.connect('my_database.duckdb')
        #duck_conn.register("temp_df", df)

        #duck_conn.execute("CREATE TABLE IF NOT EXISTS Person AS SELECT * FROM temp_df")

        # Verify by reading back
        df_stored = duck_conn.execute("SELECT * FROM Person").fetchdf()
        print(df_stored)
        

    except Exception as e:
        print("Error:", e)
    finally: 
        conn.close()
        duck_conn.close()


if __name__=="__main__":
    main()
    