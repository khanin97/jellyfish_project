from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import sys
import os

# Add 'dags/etl/' to PYTHONPATH for custom import
sys.path.append(os.path.join(os.path.dirname(__file__), 'etl'))

# ===== IMPORT ETL FUNCTION =====
from download_sst import download_sst
from convert_sst import convert_sst_nc_to_csv
from load_sst_to_duckdb import load_sst_csv_to_duckdb

from download_thetao import download_thetao
from convert_thetao import convert_thetao_nc_to_csv
from load_thetao_to_duckdb import load_thetao_csv_to_duckdb

from download_zooplankton import download_zooplankton
from convert_zooplankton import convert_zooplankton_nc_to_csv
from load_zooplankton_to_duckdb import load_zooplankton_csv_to_duckdb

from download_current import download_current
from convert_current import convert_current_nc_to_csv
from load_current_to_duckdb import load_current_csv_to_duckdb

from download_phytoplankton import download_phytoplankton
from convert_phytoplankton import convert_phytoplankton_nc_to_csv
from load_phytoplankton_to_duckdb import load_phytoplankton_csv_to_duckdb

from download_so import download_so
from convert_so import convert_so_nc_to_csv
from load_so_to_duckdb import load_so_csv_to_duckdb
# =================================

with DAG(
    dag_id="marine_data_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["marine", "etl", "cmems"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("sst_etl") as sst_group:
        t1 = PythonOperator(task_id="download_sst", python_callable=download_sst)
        t2 = PythonOperator(task_id="convert_sst", python_callable=convert_sst_nc_to_csv)
        t3 = PythonOperator(task_id="load_sst", python_callable=load_sst_csv_to_duckdb)
        t1 >> t2 >> t3

    with TaskGroup("thetao_etl") as thetao_group:
        t1 = PythonOperator(task_id="download_thetao", python_callable=download_thetao)
        t2 = PythonOperator(task_id="convert_thetao", python_callable=convert_thetao_nc_to_csv)
        t3 = PythonOperator(task_id="load_thetao", python_callable=load_thetao_csv_to_duckdb)
        t1 >> t2 >> t3

    with TaskGroup("zooplankton_etl") as zoo_group:
        t1 = PythonOperator(task_id="download_zoo", python_callable=download_zooplankton)
        t2 = PythonOperator(task_id="convert_zoo", python_callable=convert_zooplankton_nc_to_csv)
        t3 = PythonOperator(task_id="load_zoo", python_callable=load_zooplankton_csv_to_duckdb)
        t1 >> t2 >> t3

    with TaskGroup("current_etl") as current_group:
        t1 = PythonOperator(task_id="download_current", python_callable=download_current)
        t2 = PythonOperator(task_id="convert_current", python_callable=convert_current_nc_to_csv)
        t3 = PythonOperator(task_id="load_current", python_callable=load_current_csv_to_duckdb)
        t1 >> t2 >> t3

    with TaskGroup("phyto_etl") as phyto_group:
        t1 = PythonOperator(task_id="download_phyto", python_callable=download_phytoplankton)
        t2 = PythonOperator(task_id="convert_phyto", python_callable=convert_phytoplankton_nc_to_csv)
        t3 = PythonOperator(task_id="load_phyto", python_callable=load_phytoplankton_csv_to_duckdb)
        t1 >> t2 >> t3

    with TaskGroup("so_etl") as so_group:
        t1 = PythonOperator(task_id="download_so", python_callable=download_so)
        t2 = PythonOperator(task_id="convert_so", python_callable=convert_so_nc_to_csv)
        t3 = PythonOperator(task_id="load_so", python_callable=load_so_csv_to_duckdb)
        t1 >> t2 >> t3

    # DAG flow
    start >> [sst_group, thetao_group, zoo_group, current_group, phyto_group, so_group] >> end
