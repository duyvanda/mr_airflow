from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from df_to_csv_mssql_operator import ToCSVMsSqlOperator
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 8, 15, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('mssql_to_csv',
          catchup=False,
          default_args=dag_params,
          schedule_interval="@once"
)

dummy_op = DummyOperator(task_id="dummy_start", dag=dag)

to_csv = ToCSVMsSqlOperator(task_id='mssql_to_csv',mssql_conn_id="1_dms_conn_id", sql="EXEC [pr_OM_RawdataSellOutPayroll_BI] @Fromdate='20210902', @Todate='20210902';", database="PhaNam_eSales_PRO", dag=dag)

dummy_op >> to_csv
