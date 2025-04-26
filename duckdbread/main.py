import os
import duckdb
import pandas as pd
import shutil

def main():
    print('duckdb read')

    duckdb_location="E:\\PROJECT\\duckdb_file"
    current_dir=os.path.dirname(os.path.abspath(__file__))
    local_filepath=f'{current_dir}\local_cache'

    try:
        if os.path.exists(local_filepath):
            os.remove(local_filepath)

        shutil.copyfile(f'{duckdb_location}\\my_database.duckdb',local_filepath)
        #duck_conn =duckdb.connect(local_filepath,read_only=True)
        duck_conn =duckdb.connect(f'{duckdb_location}\\my_database.duckdb',read_only=True)
        # Verify by reading back
        df_stored = duck_conn.execute("SELECT * FROM 'Production.WorkOrder'").fetchdf()
        print(df_stored)
    except Exception as e:
        print("Error:", e)
    finally: 
        duck_conn.close()


if __name__=="__main__":
    main()
    