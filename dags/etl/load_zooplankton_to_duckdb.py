import os
import duckdb

def load_zooplankton_csv_to_duckdb():
    db_path = "/opt/airflow/db/jellyfish.duckdb"
    csv_folder = "/opt/airflow/csv/zooplankton"

    if not os.path.exists(csv_folder):
        raise FileNotFoundError(f"❌ Missing folder: {csv_folder}")
    if not os.path.exists(os.path.dirname(db_path)):
        raise FileNotFoundError(f"❌ Missing db folder: {os.path.dirname(db_path)}")

    con = duckdb.connect(db_path)

    for fname in os.listdir(csv_folder):
        if fname.endswith(".csv"):
            path = os.path.join(csv_folder, fname)
            table_name = os.path.splitext(fname)[0]

            con.execute(f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM read_csv_auto('{path}')
            ''')
            print(f"✅ Loaded zooplankton to DuckDB: {table_name}")

    con.close()
