
from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='BaoCaoSales'
prefix='Sales'
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
          schedule_interval= '*/15 8-17,23-23 * * *',
          tags=[prefix+name, 'Daily', '15_mins']
)

def etl_to_postgres():
    # day_ago = 2
    datenow = datetime.now().strftime("%Y%m%d")
    # datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y%m%d")
    # param_1 = f"'{datenow_day_ago}'"
    # param_2 = f"'20210901'"
    param_3 = f"'{datenow}'"
    # param_4 = f"'20211109'"

    query = f"EXEC [pr_OM_RawdataSellOutPayroll_BI_v1] @Fromdate={param_3}, @Todate={param_3}"

    FINAL = get_ms_df(sql=query)

    FINAL.columns = cleancols(FINAL)

    FINAL.NgayGiaoHang.fillna(datetime(1900, 1, 1), inplace=True)

    FINAL['phanloaispcl'] = FINAL['MaSanPham'].map(
        df_to_dict(get_ps_df("select masanpham, phanloai from d_nhom_sp where nhomsp='SPCL'"))
    ).fillna('Khác')

    FINAL['nhomsp'] = FINAL['MaSanPham'].map(
        df_to_dict(get_ps_df("select masanpham, nhomsp from d_nhom_sp where nhomsp IN ('SPCL', 'SP MOI') "))
    ).fillna('Khác')

    FINAL['khuvucviettat'] = FINAL['TenKhuVuc'].map(
        df_to_dict(get_ps_df("select * from d_mkv_viet_tat"))
    )

    FINAL['chinhanh'] = FINAL['MaCongTyCN'].map(
        df_to_dict(get_ps_df("select * from d_chi_nhanh"))
    )

    FINAL['newhco'] = (FINAL['MaKenhPhu']+FINAL['MaPhanLoaiHCO']).map(
        df_to_dict(get_ps_df("SELECT concat(makenhphu, maphanloaihco) as concat, new_mahco FROM d_pl_hco"))
    )

    FINAL['phanam'] = FINAL['MaSanPham'].map(
        df_to_dict(get_ps_df("select masanpham, nhomsp from d_nhom_sp where nhomsp='PHA NAM'"))).fillna('Merap')

    FINAL['thang'] = FINAL['NgayChungTu'] + pd.offsets.Day() - pd.offsets.MonthBegin()

    FINAL['inserted_at'] = datetime.now()

    pk = ['macongtycn', 'ngaychungtu', 'sodondathang', 'masanpham', 'solo', 'lineref', 'soluong']

    execute_values_upsert(FINAL, 'f_sales', pk=pk)



dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_etl_to_postgres = PythonOperator(task_id="etl_to_postgres", python_callable=etl_to_postgres, dag=dag)


# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

# tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Tiền Mặt', dag=dag)


dummy_start >> py_etl_to_postgres
# >> tab_refresh
