
import duckdb
import pandas as pd
import io

DB_PATH = "/opt/airflow/db/jellyfish.duckdb"
XCOM_KEY = "sst_df"

province_map = {
    "rayong_1": "rayong_1",
    "rayong_2": "rayong_2",
    "chanthaburi_1": "chanthaburi",
    "trat_1": "trat"
}

table_list = [
    "rayong_1_sst",
    "rayong_2_sst",
    "chanthaburi_1_sst",
    "trat_1_sst"
]

def load_from_db(ti):
    con = duckdb.connect(DB_PATH)
    df_list = []
    for table in table_list:
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        prefix = "_".join(table.split("_")[:2])
        df["province"] = province_map[prefix]
        df_list.append(df)
    df_all = pd.concat(df_list).reset_index(drop=True)
    ti.xcom_push(key=XCOM_KEY, value=df_all.to_json())

def transform(ti):
    df = pd.read_json(io.StringIO(ti.xcom_pull(key=XCOM_KEY)))
    if df["sst"].mean() > 100:
        df["sst"] = df["sst"] - 273.15
    df["time"] = pd.to_datetime(df["time"], errors='coerce')
    df = df.dropna(subset=["sst", "time"])
    ti.xcom_push(key=XCOM_KEY, value=df.reset_index(drop=True).to_json())

def clean(ti):
    df = pd.read_json(io.StringIO(ti.xcom_pull(key=XCOM_KEY)))
    df_daily = df.groupby([df["time"].dt.date, "province"])[sst].mean().reset_index()
    df_daily.rename(columns={"time": "date", "sst": "sst_mean"}, inplace=True)
    ti.xcom_push(key=XCOM_KEY, value=df_daily.reset_index(drop=True).to_json())

def save_to_db(ti):
    df = pd.read_json(io.StringIO(ti.xcom_pull(key=XCOM_KEY)))
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE province_daily_sst AS SELECT * FROM df")
    print("✅ Overwritten: province_daily_sst in DuckDB")
