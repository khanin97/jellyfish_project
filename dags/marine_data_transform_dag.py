from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import pendulum

# import modules from tf_cls
from tf_cls import tf_sst, tf_thetao, tf_zooplankton, tf_phytoplankton, tf_current, tf_so

def make_taskgroup(name, module):
    with TaskGroup(group_id=f"transform_{name}") as tg:
        t1 = PythonOperator(task_id="load", python_callable=module.load_from_db)
        t2 = PythonOperator(task_id="transform", python_callable=module.transform)
        t3 = PythonOperator(task_id="clean", python_callable=module.clean)
        t4 = PythonOperator(task_id="save", python_callable=module.save_to_db)
        t1 >> t2 >> t3 >> t4
    return tg

with DAG(
    dag_id="marine_data_transform_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Bangkok"),
    schedule_interval=None,
    catchup=False,
    tags=["marine", "transform"],
    description="Transform and clean all marine datasets into DuckDB"
) as dag:

    tg_sst = make_taskgroup("sst", tf_sst)
    tg_thetao = make_taskgroup("thetao", tf_thetao)
    tg_zooplankton = make_taskgroup("zooplankton", tf_zooplankton)
    tg_phytoplankton = make_taskgroup("phytoplankton", tf_phytoplankton)
    tg_current = make_taskgroup("current", tf_current)
    tg_so = make_taskgroup("so", tf_so)

    tg_sst >> tg_thetao >> tg_zooplankton >> tg_phytoplankton >> tg_current >> tg_so
