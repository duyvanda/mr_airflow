from datetime import datetime
import os
from google.oauth2 import service_account
from googleapiclient import discovery
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 8, 15, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'do_xcom_push': False
}

dag = DAG('hello_gdive',
          catchup=False,
          default_args=dag_params,
          schedule_interval="@once",
          tags=['admin', 'test'],
)

def print_file_id():
    # Google Sheet connection
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file']
    path = "/home/biserver/airflow/dags/files/"
    jsonfile = path+"datateam1599968716114-6f9f144b4262.json"
    credentials = service_account.Credentials.from_service_account_file(jsonfile, scopes=scopes)
    service = discovery.build('drive', 'v3', credentials=credentials, cache_discovery=False)
    folder_id = '1nTlohjovIm7e2pf2LSHf3wGnyuuStGbh'
    query = f"parents = '{folder_id}'"
    files = service.files().list(q=query).execute()
    fileid  = files['files'][0]['id']
    print("I LOVE PYTHON: "+fileid)


dummy_op = DummyOperator(task_id="dummy_op", dag=dag)

py_op_1 = PythonOperator(task_id="py_op_1",
                         python_callable=print_file_id,
                         dag=dag)


dummy_op >> py_op_1