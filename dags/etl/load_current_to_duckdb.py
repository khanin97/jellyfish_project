import os
import duckdb

def load_current_csv_to_duckdb():
    db_path = "/opt/airflow/db/jellyfish.duckdb"
    csv_folder = "/opt/airflow/csv/current"

    # ❗ ตรวจว่ามีโฟลเดอร์ที่จำเป็นครบ
    if not os.path.exists(csv_folder):
        raise FileNotFoundError(f"❌ Missing folder: {csv_folder}")
    if not os.path.exists(os.path.dirname(db_path)):
        raise FileNotFoundError(f"❌ Missing db folder: {os.path.dirname(db_path)}")

    con = duckdb.connect(db_path)

    for fname in os.listdir(csv_folder):
        if fname.endswith(".csv"):
            path = os.path.join(csv_folder, fname)
            table_name = fname.replace(".csv", "")

            # ✅ เขียนทับตารางใน DuckDB
            con.execute(f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM read_csv_auto('{path}')
            ''')

            print(f"✅ Loaded current to DuckDB: {table_name}")

    con.close()
