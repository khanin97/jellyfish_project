
import duckdb
import pandas as pd
import io

DB_PATH = "/opt/airflow/db/jellyfish.duckdb"
XCOM_KEY = "current_df"

province_map = {
    "rayong_1": "rayong_1",
    "rayong_2": "rayong_2",
    "chanthaburi_1": "chanthaburi",
    "trat_1": "trat"
}

table_list = [
    "rayong_1_current",
    "rayong_2_current",
    "chanthaburi_1_current",
    "trat_1_current"
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
    import numpy as np
    df["time"] = pd.to_datetime(df["time"], errors='coerce')
    uo_cols = [col for col in df.columns if col.startswith("uo_")]
    vo_cols = [col for col in df.columns if col.startswith("vo_")]
    if len(uo_cols) != len(vo_cols):
        raise ValueError("จำนวนคอลัมน์ uo_* และ vo_* ไม่เท่ากัน")
    speed_df = pd.DataFrame()
    for uo, vo in zip(uo_cols, vo_cols):
        depth = uo.split('_')[-1]
        speed_df[f"speed_{{depth}}"] = np.sqrt(df[uo] ** 2 + df[vo] ** 2)
    df["speed"] = speed_df.mean(axis=1)
    df = df.dropna(subset=["time", "speed", "province"])
    ti.xcom_push(key=XCOM_KEY, value=df.reset_index(drop=True).to_json())

def clean(ti):
    df = pd.read_json(io.StringIO(ti.xcom_pull(key=XCOM_KEY)))
    df_daily = df.groupby([df["time"].dt.date, "province"])[speed].mean().reset_index()
    df_daily.rename(columns={"time": "date", "speed": "current_mean"}, inplace=True)
    ti.xcom_push(key=XCOM_KEY, value=df_daily.reset_index(drop=True).to_json())

def save_to_db(ti):
    df = pd.read_json(io.StringIO(ti.xcom_pull(key=XCOM_KEY)))
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE province_daily_current AS SELECT * FROM df")
    print("✅ Overwritten: province_daily_current in DuckDB")
