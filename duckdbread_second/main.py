import os
import duckdb
import pandas as pd

def main():
    print('duckdb read')

    duckdb_location="E:\\PROJECT\\duckdb_file"

    try:
        duck_conn =duckdb.connect(f'{duckdb_location}\\my_database.duckdb',read_only=True)
        # Verify by reading back
        df_stored = duck_conn.execute("SELECT * FROM 'Sales.SalesOrderDetail'").fetchdf()
        print(df_stored)
    except Exception as e:
        print("Error:", e)
    finally: 
        duck_conn.close()


if __name__=="__main__":
    main()
    