import os
import duckdb
import pyodbc
import pandas as pd
import sqlite3

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
    { "table_name":"[Person].[Person]", "where_condition":"1=1", "select_condition": "top 50000 * "},
        { "table_name":"[Production].[WorkOrder]", "where_condition":"1=1", "select_condition": "top 10000 * "},
        { "table_name":"[Sales].[SalesOrderDetail]", "where_condition":"UnitPrice>20", "select_condition": "top 20000 * "},
        { "table_name":"[Sales].[CreditCard]", "where_condition":"1=1", "select_condition": " * "}
    ]

    try:

        sqllite_con =sqlite3.connect(f'{duckdb_location}\\sqllite_test.db')
        conn = pyodbc.connect(conn_str)
        
        for table in table_list:
            query = f'select {table["select_condition"]} from {table["table_name"]} where {table["where_condition"]}'
            df = pd.read_sql(query, conn)
            print(df)
            df.to_sql(table["table_name"], sqllite_con, if_exists='replace', index=False)

            # Verify by reading back
            df_stored=pd.read_sql(f"SELECT * FROM '{table["table_name"]}'", sqllite_con)
            #df_stored = duck_conn.execute(f"SELECT * FROM {table["table_name"]}").fetchdf()
            print(df_stored)

    except Exception as e:
        print("Error:", e)
    finally: 
        conn.close()
        sqllite_con.close()


if __name__=="__main__":
    main()
    