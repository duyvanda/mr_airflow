from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from hello_operator import HelloOperator

import pendulum

def writefile():
    f = open("myfile.txt", "a")
    f.write("\nNow the file has more content! %s" % datetime.now())
    f.close()
    print('Done Writing')
    print(os.getcwd())

local_tz = pendulum.timezone("Asia/Bangkok")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 8, 15, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'do_xcom_push': False
}

dag = DAG('name_writefile',
          catchup=False,
          default_args=dag_params,
          schedule_interval="@once",
          tags=['admin', 'test'],
)

dummy_op = DummyOperator(task_id="dummy_start", dag=dag)

py_op_1 = PythonOperator(task_id="writefile",
                         python_callable=writefile,
                         dag=dag)
hello_task = HelloOperator(task_id='sample-task', name='THIS IS FROM CUSTOM OPERATOR', dag=dag)

hello_task2 = HelloOperator(task_id='sample-task-2', name='I LOVE AIRFLOW', dag=dag)

dummy_op >> py_op_1 >> hello_task >> hello_task2