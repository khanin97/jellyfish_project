import os
import duckdb

def load_phytoplankton_csv_to_duckdb():
    db_path = "db/jellyfish.duckdb"
    os.makedirs("db", exist_ok=True)
    con = duckdb.connect(db_path)

    folder = "csv/phytoplankton"
    for fname in os.listdir(folder):
        if fname.endswith(".csv"):
            path = os.path.join(folder, fname)
            table_name = fname.replace(".csv", "")
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path}');")
            print(f"✅ Loaded phytoplankton to DuckDB: {table_name}")

    con.close()
