import sys
from src.policys.etl_policys import Policys

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta

START_DATE = days_ago(1)

db_host = "postgres"
db_name = "db_policys"
db_user = "airflow"
db_password = "airflow"


def etl():
    try:
        pf = Policys(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
        )
        pf.run()
        print("****************** Success process ETL of data of csv")
    except:
        print(
            "****************** Unexpected error on ETL: ",
            sys.exc_info(),
        )
        raise


default_args = {
    "owner": "daniel.cristancho",
    "depends_on_past": False,
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "max_active_runs": 1,
}

dag = DAG(
    "dag.test_monokera",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 12 * * *",
    start_date=START_DATE,
)

dag_start = DummyOperator(
    task_id="start",
    dag=dag,
)

dag_end = DummyOperator(
    task_id="end",
    dag=dag,
)

task_etl = PythonOperator(
    task_id="task_etl",
    python_callable=etl,
    dag=dag,
)

(dag_start >> task_etl >> dag_end)
