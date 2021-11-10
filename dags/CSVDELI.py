
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='DELI'
prefix='CSV'
csv_path = '/home/biserver/data_lake/'+prefix+name+'/'

dag_params = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          schedule_interval= '*/10 8-17,22-22 * * *',
          tags=[prefix+name, 'Daily', '10_mins']
)

def get_csv_files():
    print(f"this is date now {datetime.now()}")

    # print(param_3)

    DELI = """
    SELECT DISTINCT
    a.BranchID,
    a.Status,
    a.OrderNbr,
    ShipDate = ISNULL(c.ShipDate, a.Crtd_DateTime)
    -- INTO #Deli
    FROM OM_Delivery a WITH (NOLOCK)

    --INNER JOIN #Sales d ON
    --d.BranchID = a.BranchID AND
    --d.OrderNbr = a.OrderNbr
    INNER JOIN
    (
    SELECT
    de.BranchID,
    de.BatNbr,
    de.OrderNbr,
    Sequence = MAX(Sequence)
    FROM dbo.OM_Delivery de
    -- INNER JOIN #Sales d
    -- ON d.BranchID = de.BranchID
    -- AND d.OrderNbr = de.OrderNbr
    GROUP BY
    de.BranchID,
    de.BatNbr,
    de.OrderNbr
    ) b ON
    b.BatNbr = a.BatNbr AND
    b.BranchID = a.BranchID AND
    b.Sequence = a.Sequence AND
    b.OrderNbr = a.OrderNbr
    LEFT JOIN
    (
    SELECT de.BranchID,
    de.BatNbr,
    de.OrderNbr,
    ShipDate = MAX(ShipDate)
    FROM dbo.OM_DeliHistory de
    -- INNER JOIN #Sales d ON
    -- d.BranchID = de.BranchID AND
    -- d.OrderNbr = de.OrderNbr
    GROUP BY
    de.BranchID,
    de.BatNbr,
    de.OrderNbr
    ) c ON
    c.BatNbr = a.BatNbr AND
    c.BranchID = a.BranchID AND
    c.OrderNbr = a.OrderNbr;
    """

    get_ms_csv(DELI, csv_path+'DELI.csv')

# transform

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_get_csv_files = PythonOperator(task_id="get_csv_files", python_callable=get_csv_files, dag=dag)


# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Tiền Mặt', dag=dag)


dummy_start >> py_get_csv_files
# >> tab_refresh
