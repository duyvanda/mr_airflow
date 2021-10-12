from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.email import EmailOperator
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 8, 15, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'do_xcom_push': False
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG('sending_email',
          catchup=False,
          default_args=dag_params,
          schedule_interval="@once",
          tags=['admin', 'test'],
)

sending_email_notification = EmailOperator(
    task_id="sending_email",
    to="airflow_course@yopmail.com",
    subject="forex_data_pipeline",
    html_content="""<h3>forex_data_pipeline succeeded</h3>""",
    dag=dag
)