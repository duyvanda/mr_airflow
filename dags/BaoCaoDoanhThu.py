import os
import numpy as np
from datetime import datetime, timedelta
from utils.df_handle import *
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

name='BaoCaoDoanhThu'
prefix='Debt'

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
          schedule_interval= '*/10 8-17,23-23 * * *',
          tags=[prefix+name, 'Daily', '10mins']
)

def etl_to_postgres():
    day_ago = 2
    datenow = datetime.now().strftime("%Y%m%d")
    datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y%m%d")
    param_1 = f"'{datenow_day_ago}'"
    # param_2 = f"'20211012'"
    param_3 = f"'{datenow}'"
    print(param_1, param_3)
    query = f"EXEC [pr_AR_TrackingDebtConfirm]  @Fromdate={param_1}, @Todate={param_3}"
    hook_ms = MsSqlHook(mssql_conn_id='1_dms_conn_id', schema='PhaNam_eSales_PRO', conn_type='odbc')
    hook_ps = PostgresHook(postgres_conn_id="postgres_con", schema="biteam")
    df1 = hook_ms.get_pandas_df(sql=query, parse_dates={"DateOfOrder": {"dayfirst": True}, "DueDate": {"dayfirst": True}})
    #
    # df_filter(df1, BranchID='MR0010', OrderNbr='DH012021-06151').to_clipboard()
    # df1.columns
    # df1[checkdup(df1, 2, ['BranchID','OrderDate','OrderNbr','DateOfOrder','DueDate'])]
    df_nhansu = hook_ps.get_pandas_df("select * from d_nhan_su")
    df_nhansu['qlkhuvuc'] = df_nhansu['qlkhuvuc'].str.strip()
    df_nhansu_asm = dropdup(df_nhansu[['manvcrscrss','qlkhuvuc']], 1)
    nhansu_asm_dict = df_to_dict(df_nhansu_asm)
    df_phu_trach_no_cs_theo_tinh = hook_ps.get_pandas_df("SELECT * from d_phu_trach_no_cs_theo_tinh")
    # df_phu_trach_no_cs_theo_tinh.columns
    phu_trach_no_cs_theo_tinh_dict = df_to_dict(df_phu_trach_no_cs_theo_tinh)
    df1['ASMName'] = np.where(df1['DebtInCharge']=='MDS', df1['SlsperID'].map(nhansu_asm_dict).fillna('Lương Trịnh Thắng (KN)'), df1['State'].map(phu_trach_no_cs_theo_tinh_dict))
    # df1['InChargeName'] =  np.where(df1['DebtInCharge']=='MDS', df1['SlsperName'], df1['ASMName'])
    # list_cs = df_phu_trach_no_cs_theo_refcustid['refcustid'].to_list()
    # ((df1['RefCustId'].isin(list_cs)) & (df1['DebtInCharge']=='CS'))
    ma_kh_cu_dict = get_ms_df("SELECT CustId, RefCustID from AR_Customer").set_index('CustId').to_dict()['RefCustID']
    df1['RefCustId'] = df1['CustID'].map(ma_kh_cu_dict)
    df_phu_trach_no_cs_theo_refcustid = hook_ps.get_pandas_df("select * from d_phu_trach_no_cs_theo_refcustid")
    phu_trach_no_cs_theo_refcustid_dict = hook_ps.get_pandas_df("select * from d_phu_trach_no_cs_theo_refcustid").set_index('refcustid').to_dict()['inchargename']
    list_cs = df_phu_trach_no_cs_theo_refcustid['refcustid'].to_list()
    df1['ASMName_CS'] = df1['RefCustId'].map(phu_trach_no_cs_theo_refcustid_dict)
    df1['ASMName'] = np.where( ((df1['RefCustId'].isin(list_cs)) & (df1['DebtInCharge']=='CS')), df1['ASMName_CS'], df1['ASMName'])
    df_nhansu_sup = dropdup(df_nhansu[['manvcrscrss','quanlytructiep']], 1)
    df_nhansu_sup['quanlytructiep'] = df_nhansu_sup['quanlytructiep'].str.strip()
    # df_to_dict(df_nhansu_sup)
    df1['SupName'] = df1['SupName'].map( df_to_dict(df_nhansu_sup) ).fillna(df1['ASMName'])
    df_nhansu_rsm = dropdup(df_nhansu[['manvcrscrss','qlvung']], 1)
    df_nhansu_rsm['qlvung'] = df_nhansu_rsm['qlvung'].str.strip()
    df1['SupName'] = df1['SupName'].map( df_to_dict(df_nhansu_sup) ).fillna(df1['ASMName'])
    df1['RSMName'] = df1['RSMName'].map( df_to_dict(df_nhansu_rsm) ).fillna(df1['ASMName'])
    # ma_kh_cu_dict = get_ms_df("SELECT CustId, RefCustID from AR_Customer").set_index('CustId').to_dict()['RefCustID']
    # df1['RefCustId'] = df1['CustID'].map(ma_kh_cu_dict)
    # df_phu_trach_no_cs_theo_refcustid = hook_ps.get_pandas_df("select * from phu_trach_no_cs_theo_refcustid")
    # phu_trach_no_cs_theo_refcustid_dict = hook_ps.get_pandas_df("select * from phu_trach_no_cs_theo_refcustid").set_index('refcustid').to_dict()['inchargename']
    # list_cs = df_phu_trach_no_cs_theo_refcustid['refcustid'].to_list()
    # df1['ASMName_CS'] = df1['RefCustId'].map(phu_trach_no_cs_theo_refcustid_dict)
    # df1['ASMName'] = np.where(df1['RefCustId'].isin(list_cs), df1['ASMName_CS'], df1['ASMName'])
    df1['InChargeName'] =  np.where(df1['DebtInCharge']=='MDS', df1['SlsperName'], df1['ASMName'])
    # vc(df1, 'ASMName')
    df_vptt = hook_ps.get_pandas_df("select * from d_vptt")
    df1 = df1.merge(df_vptt, how = 'left', left_on='State', right_on='tinh',suffixes=('_left', '_right'), validate="m:1")
    df_mkv_viet_tat = hook_ps.get_pandas_df("select * from d_mkv_viet_tat").set_index('tenkhuvuc')
    khuvuc_dict = df_mkv_viet_tat.to_dict()['khuvuc']
    df1['Territory'] = df1['Territory'].map(khuvuc_dict)
    df1['Position'] = np.where(df1['DebtInCharge']=="CS", "CS", df1['Position'])
    groupbylist = [
        'OrderNbr', 
        'BranchID', 
        'Position',
        'SlsperID', 
        'SupName', 
        'ASMName', 
        'RSMName', 
        'DateOfOrder', 
        'DueDate',
        'CustID',
        'RefCustId',
        'CustName',
        'SlsperName',
        'InChargeName',
        'DebtInCharge',
        'Terms', 
        'PaymentsForm',
        'TermsType',
        'Territory', 
        'State', 
        'vptt', 
        'DeliveryUnit',
        'Channels',
        'SubChannel'
        ]
    aggregate_dict = {
    'OrderDate': np.max,
    #Group by tien
    'OpeiningOrderAmt':np.sum,
    'OrdAmtRelease':np.sum,
    'DeliveredOrderAmt':np.sum,
    'ReturnOrdAmt': np.sum,
    'DebConfirmAmt': np.sum,
    'DebConfirmAmtRelease': np.sum,
    #DonHang8
    'CountOpeningOrder':np.sum,
    'CountOrdRelease': np.sum,
    'DeliveredOrder':np.sum,
    'CountReturnOrd': np.sum,
    'CountDebtConfirm': np.sum,
    'CountDebtConfirmRelease':np.sum
    }
    df2 = pivot(df1, groupbylist, aggregate_dict)
    # df2.columns
    rename_dict ={
    # Doanh So
    'OpeiningOrderAmt': 'tiennodauky',
    'OrdAmtRelease':'tienchotso',
    'DeliveredOrderAmt':'tiengiaothanhcong',
    'ReturnOrdAmt':'tienhuydon',
    'DebConfirmAmt':'tienlenbangke',
    'DebConfirmAmtRelease':'tienthuquyxacnhan',
    # Don Hang
    'CountOpeningOrder': 'dondauky',
    'CountOrdRelease':'donchotso',
    'DeliveredOrder':'dongiaothanhcong',
    'CountReturnOrd':'donhuy',
    'CountDebtConfirm':'donlenbangke',
    'CountDebtConfirmRelease':'donthuquyxacnhan'
    }
    df2.rename(columns=rename_dict, inplace=True)
    df2['tiennocongty'] = df2['tiennodauky'] + df2['tienchotso'].values - df2['tienhuydon'].values - df2['tienthuquyxacnhan'].values
    # df2['tiennocongty'] = np.where(df2['chitietnocongty']>0, df2['chitietnocongty'].values, 0)
    # df2['chitietdonnocongty'] = df2['dondauky'] + df2['donchotso'] - df2['donthuquyxacnhan'] - df2['donhuy']
    # df2['donnocongty_old'] = df2.apply(lambda x: x['chitietdonnocongty'] if x['chitietdonnocongty']>0 else 0, axis=1)
    df2['donnocongty'] = np.where(df2['tiennocongty'].values > 0, 1, 0)

    df2['donchuagiao'] = df2['donchotso'] - df2['dongiaothanhcong'] - df2['donhuy']
    df2['tiendonchuagiao'] = df2['tienchotso'] - df2['tiengiaothanhcong'] - df2['tienhuydon']

    # date_now_at_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # df2['OverDueDate'] = np.where(df2['DueDate'].values <= np.datetime64(date_now_at_midnight), True, False)
    # # df2['OverDueDate'].value_counts()
    # df2['nodenhan'] = np.where(df2['OverDueDate'].values, df2['tiennocongty'].values, 0)
    # df2['nochuadenhan'] = np.where(df2['OverDueDate'].values != True, df2['tiennocongty'].values, 0)
    # df2[checkdup(df2, 2, ['BranchID','OrderNbr','DateOfOrder'])].to_clipboard()
    # .to_clipboard()
    # df2.columns
    # insert_df_to_postgres(tbl_name="f_tracking_debt", df=df2, primary_keys=['ordernbr', 'branchid', 'dateoforder', 'duedate'])
    primary_keys=['ordernbr', 'branchid', 'dateoforder', 'duedate']
    rows = list(df2.itertuples(index=False, name=None))
    hook_ps.insert_rows(table='f_tracking_debt', rows=rows, replace=True, target_fields=[x.lower() for x in df2.columns],replace_index=primary_keys)
#
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_etl_to_postgres = PythonOperator(task_id="etl_to_postgres", python_callable=etl_to_postgres, dag=dag)

# hello_task4 = ToCSVMsSqlOperator(task_id='sample-task-4', mssql_conn_id="1_dms_conn_id", sql=sql, database="PhaNam_eSales_PRO", path=path, dag=dag)

tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Tiền Mặt', dag=dag)



dummy_start >> py_etl_to_postgres >> tab_refresh
