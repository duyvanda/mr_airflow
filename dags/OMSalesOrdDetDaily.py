from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from hello_operator import HelloOperator
from hello_mssql_operator import HelloMsSqlOperator
from mssql_to_csv_operator import ToCSVMsSqlOperator
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

name= 'SalesOrdDet'
prefix='OM'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'do_xcom_push': False
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '*/10 8-17,23-23 * * *',
          tags=[prefix+name, 'Daily', '10mins']
)

datenow = datetime.now().strftime(r"%Y-%m-%d")
datenow_file = datetime.now().strftime(r"%Y%m%d")


param_1 = f"'{datenow}'"

def print_file():
    print(param_1)

path = f'/home/biserver/data_lake/{prefix}_{name}/{prefix}_{name}_{datenow_file}.csv' 

sql=f"SELECT * FROM {prefix}_{name} WHERE CAST (Crtd_Datetime As [Date]) <= {param_1} and CAST (Crtd_Datetime As [Date]) >= {param_1}"

# TASK HERE

dummy_op = DummyOperator(task_id="dummy_start", dag=dag)

py_op_1 = PythonOperator(task_id="print", python_callable=print_file, dag=dag)

to_csv = ToCSVMsSqlOperator(task_id='to_csv', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# END TASK
dummy_op >> py_op_1 >> to_csv
