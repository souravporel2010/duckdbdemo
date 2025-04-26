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
    duckdb_location="E:\\PROJECT\\duckdb_file"

    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    table_list = [
        { "table_name":"Person.Person", "where_condition":"1=1", "select_condition": "top 50000 * "},
        { "table_name":"Production.WorkOrder", "where_condition":"1=1", "select_condition": "top 10000 * "},
        { "table_name":"Sales.SalesOrderDetail", "where_condition":"UnitPrice>20", "select_condition": "top 20000 * "},
        { "table_name":"Sales.CreditCard", "where_condition":"1=1", "select_condition": " * "}
    ]

    try:

        duck_conn =duckdb.connect(f'{duckdb_location}\\my_database.duckdb')
        conn = pyodbc.connect(conn_str)
        
        for table in table_list:
            query = f'select {table["select_condition"]} from {table["table_name"]} where {table["where_condition"]}'
            df = pd.read_sql(query, conn)
            print(df)
            duck_conn.register("temp_df", df)
            duck_conn.execute(f"DROP TABLE IF EXISTS {table["table_name"]}")
            duck_conn.execute(f"CREATE TABLE IF NOT EXISTS '{table["table_name"]}' as select * from temp_df")

            # Verify by reading back
            df_stored = duck_conn.execute(f"SELECT * FROM {table["table_name"]}").fetchdf()
            print(df_stored)

    except Exception as e:
        print("Error:", e)
    finally: 
        conn.close()
        duck_conn.close()


if __name__=="__main__":
    main()
    