import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow import DAG

with DAG(
    dag_id="codeforces_loading_dag",
    start_date=datetime(2015, 12, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['cf-dg'],
) as dag:

    execution_start = BashOperator(
        task_id="execution_start",
        bash_command=f"echo Starting the pipeline execution!"
    )

    stg_dimensions_load = BashOperator(
        task_id="stg_dimensions_load",
        bash_command=f"python3 /opt/airflow/jobs/dimensions-stg.py --db-hostname={os.getenv('DW_HOSTNAME')} --sql-conn-version={os.getenv('SQL_CONN_VERSION')} --dw-port-number=3306"
    )

    load_submissions = MySqlOperator(
        task_id="load_submissions", sql=r"""CALL InsertSubmissions()"""
    )

    merge_stg_prod = MySqlOperator(
        task_id="merge_stg_prod", sql=r"""CALL MergeStgToProd()"""
    )

    execution_start >> stg_dimensions_load  >> load_submissions >> merge_stg_prod
