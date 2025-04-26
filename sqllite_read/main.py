import os
import duckdb
import pandas as pd
import sqlite3

def main():
    print('duckdb read')

    duckdb_location="E:\\PROJECT\\duckdb_file"

    try:
        sqllite_conn =sqlite3.connect(f'{duckdb_location}\\sqllite_test.db',read_only=True)
        # Verify by reading back
        df_stored = pd.read_sql("SELECT * FROM 'Sales.SalesOrderDetail",sqllite_conn)
        print(df_stored)
    except Exception as e:
        print("Error:", e)
    finally: 
        duck_conn.close()


if __name__=="__main__":
    main()
    