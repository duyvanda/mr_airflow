from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from hello_operator import HelloOperator
from hello_mssql_operator import HelloMsSqlOperator
from mssql_to_csv_operator import ToCSVMsSqlOperator
from from_df_to_postgres_operator import InsertToPostgresOperator

import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'do_xcom_push': False
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG('hello_world',
          catchup=False,
          default_args=dag_params,
          schedule_interval= '@once',
          tags=['admin', 'test']
)

datenow = datetime.now().strftime(r"%Y-%m-%d")
datenow_file = datetime.now().strftime(r"%Y%m%d")


param_1 = f"'{datenow}'"

def print_file():
    print(param_1)

path = f'/home/biserver/data_lake/OM_SalesOrd/OM_SalesOrd_{datenow_file}.csv' 

sql=f"SELECT * FROM OM_SalesOrd WHERE CAST (OrderDate As [Date]) <= {param_1} and CAST (OrderDate As [Date]) >= {param_1}"

dummy_op = DummyOperator(task_id="dummy_start", dag=dag)

py_op_1 = PythonOperator(task_id="print", python_callable=print_file, dag=dag)

hello_task = HelloOperator(task_id='sample-task', name='THIS IS FROM CUSTOM OPERATOR', dag=dag)

hello_task2 = HelloOperator(task_id='sample-task-2', name='I LOVE AIRFLOW', dag=dag)

hello_task3 = HelloMsSqlOperator(task_id='sample-task-3',mssql_conn_id="1_dms_conn_id", sql="SELECT @@version;", database="PhaNam_eSales_PRO", dag=dag)

hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

hello_task5 = InsertToPostgresOperator(task_id='sample-task-5', postgres_conn_id="postgres_con", sql=None, database="biteam", dag=dag)


dummy_op >> py_op_1 >> hello_task >> hello_task2 >> hello_task3 >> hello_task4 >> hello_task5
