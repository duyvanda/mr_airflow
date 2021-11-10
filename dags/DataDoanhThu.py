from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


local_tz = pendulum.timezone("Asia/Bangkok")

name='DataDoanhThu'
prefix='Debt_'
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
          schedule_interval= '*/15 5-23 * * *',
          tags=[prefix+name, 'Daily', '15mins']
)

def update_table():
    datenow = datetime.now().strftime("%Y%m%d")
    query = f"EXEC pr_AR_RawdataTrackingDebtConfirm_BL 2,'{datenow}','{datenow}','';"
    commit_ms_sql(query)
    query = f"SELECT BranchID, OrderNbr, InvcNbr, InvcNote, DateOfOrder, DueDate, CustID FROM AR_RawdataTrackingDebtConfirm_BL WHERE Terms=N'Gối 1 Đơn Hàng (trong 30 ngày)'"
    df1 = get_ms_df(sql=query, parse_dates={"DateOfOrder": {"dayfirst": True}, "DueDate": {"dayfirst": True}})
    df1a=df1.copy()
    df1a.DueDate = df1a.DateOfOrder+timedelta(30)
    # df1a.shape
    df1a.sort_values(['DateOfOrder', 'OrderNbr'], axis=0, ascending = (True, True), inplace=True)
    df1b = df1a[['BranchID','CustID', 'OrderNbr']].copy()
    df1b.drop_duplicates(keep='first', inplace=True)
    # df1b.shape
    df1b['cum_count'] = df1b.groupby(['BranchID','CustID']).cumcount()
    df1a = df1a.merge(df1b, how='left', on=['BranchID','CustID', 'OrderNbr'])
    df1a['cum_count_plus1'] = df1a['cum_count']+1
    df1b = df1a[['BranchID', 'CustID', 'cum_count', 'OrderNbr', 'DateOfOrder']].drop_duplicates(keep='first')
    # df1b.to_csv('df1b.csv')
    df1a = df1a.merge(df1b.add_prefix('y_'), how='left', left_on=['BranchID', 'CustID', 'cum_count_plus1'], right_on=['y_BranchID','y_CustID','y_cum_count'])
    df1a.y_DateOfOrder.fillna(df1a.DueDate, inplace=True)
    df1a.DueDate = np.where(df1a.DueDate>df1a.y_DateOfOrder, df1a.y_DateOfOrder, df1a.DueDate)
    cols_list = df1.columns
    del(df1)
    df1a = df1a[cols_list]
    df1a.drop_duplicates(keep='first', inplace=True)
    df1a.drop('CustID', axis=1, inplace=True)
    # df1a.columns
    update_sql = \
    """
    UPDATE d SET d.DueDate=t.DueDate
    FROM  dbo.AR_RawdataTrackingDebtConfirm_BL d
    INNER JOIN #tempt1 t ON t.BranchID = d.BranchID AND t.OrderNbr = d.OrderNbr AND t.InvcNbr = d.InvcNbr AND t.InvcNote = d.InvcNote
    """
    insert_to_ms_and_update(df1a, '#tempt1', update_sql)

# transform

def etl_to_postgres():
    if datetime.now().hour in {8,9,10,11,12,13,14,15,16,17,18,23}:
        # day_ago = 3
        datenow = datetime.now().strftime("%Y%m%d")
        # datenow_day_ago = ( datetime.now()-timedelta(day_ago) ).strftime("%Y%m%d")
        # param_1 = f"'{datenow_day_ago}'"
        param_2 = f"'20161130'"
        param_3 = f"'{datenow}'"
        print(param_2, param_3)
        query = f"EXEC [pr_AR_TrackingDebtConfirm]  @Fromdate={param_2}, @Todate={param_3}"
        hook_ms = MsSqlHook(mssql_conn_id='1_dms_conn_id', schema='PhaNam_eSales_PRO', conn_type='odbc')
        hook_ps = PostgresHook(postgres_conn_id="postgres_con", schema="biteam")
        df1 = hook_ms.get_pandas_df(sql=query, parse_dates={"DateOfOrder": {"dayfirst": True}, "DueDate": {"dayfirst": True}, "OrderDate": {"dayfirst": True}} )

        # Update 29/10
        # ctr1 = df1.Terms == 'Gối 1 Đơn Hàng (trong 30 ngày)'
        # df1a=df1[ctr1].copy()
        # df1a.DueDate = df1a.DateOfOrder+timedelta(30)
        # df1a.sort_values('DateOfOrder', axis=0, ascending = True, inplace=True)
        # df1b = df1a[['BranchID','CustID','OrderNbr']].copy()
        # df1b.drop_duplicates(keep='first', inplace=True)
        # df1b['cum_count'] = df1b.groupby(['BranchID','CustID']).cumcount()
        # df1a = df1a.merge(df1b, how='left', on=['BranchID','CustID', 'OrderNbr'])
        # df1a['cum_count_plus1'] = df1a['cum_count']+1
        # df1a = df1a.merge(df1a[['BranchID', 'CustID', 'cum_count', 'OrderNbr', 'DateOfOrder']].drop_duplicates(keep='first').add_prefix('y_'), how='left', left_on=['BranchID', 'CustID', 'cum_count_plus1', 'OrderNbr'], right_on=['y_BranchID','y_CustID','y_cum_count', 'y_OrderNbr'])
        # df1a.y_DateOfOrder.fillna(df1a.DueDate, inplace=True)
        # df1a.DueDate = np.where(df1a.DueDate>df1a.y_DateOfOrder, df1a.y_DateOfOrder, df1a.DueDate)
        # cols_list = df1.columns
        # df1a = df1a[cols_list]
        # df1 = df1[~ctr1].append(df1a)
        # del(df1a)
        # del(df1b)


        
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

        # df1['Action_MY'] = df1['OrderDate'].dt.month.astype('str')+df1['OrderDate'].dt.year.astype('str')

        # df1['Order_MY'] = df1['DateOfOrder'].dt.month.astype('str')+df1['DateOfOrder'].dt.year.astype('str')

        # df1['ReturnOrdAmt_InMonth'] = np.where( df1['Order_MY']==df1['Action_MY'], df1['ReturnOrdAmt'], 0)
        # df1['DebConfirmAmtRelease_InMonth'] = np.where( df1['Order_MY']==df1['Action_MY'], df1['DebConfirmAmtRelease'], 0)

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
        'OpeningOrderAmt':np.sum,
        'OrdAmtRelease':np.sum,
        'DeliveredOrderAmt':np.sum,
        'ReturnOrdAmt': np.sum,
        'DebtConfirmAmt': np.sum,
        'DebtConfirmAmtRelease': np.sum,
        # Huy & Xac Nhan In Month
        # 'ReturnOrdAmt_InMonth': np.sum,
        # 'DebConfirmAmtRelease_InMonth': np.sum,

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
        'OpeningOrderAmt': 'tiennodauky',
        'OrdAmtRelease':'tienchotso',
        'DeliveredOrderAmt':'tiengiaothanhcong',
        'ReturnOrdAmt':'tienhuydon',
        'DebtConfirmAmt':'tienlenbangke',
        'DebtConfirmAmtRelease':'tienthuquyxacnhan',
        # Huy & Xac Nhan In Month
        # 'ReturnOrdAmt_InMonth': 'tienhuydon_inmonth',
        # 'DebConfirmAmtRelease_InMonth': 'tienthuquyxacnhan_inmonth',
        # Don Hang
        'CountOpeningOrder': 'dondauky',
        'CountOrdRelease':'donchotso',
        'DeliveredOrder':'dongiaothanhcong',
        'CountReturnOrd':'donhuy',
        'CountDebtConfirm':'donlenbangke',
        'CountDebtConfirmRelease':'donthuquyxacnhan'
        }

        df2.rename(columns=rename_dict, inplace=True)
        # update 04/11
        ctr1 = df2['DeliveryUnit']=='Nhà vận chuyển'
        ctr2 = df2['donhuy']==0
        df2['dongiaothanhcong'] = np.where(ctr1&ctr2, 1, df2['dongiaothanhcong'])

        # update 05/11
        ctr1 = df2['DeliveryUnit']=='Nhà vận chuyển'
        ctr2 = df2['tienhuydon']==0
        df2['tiengiaothanhcong'] = np.where(ctr1&ctr2, df2['tienchotso'], df2['tiengiaothanhcong'])

        trangthaigiaohangdict = {'C':'Đã giao hàng', 'H':'Chưa Xác Nhận', 'D':'KH Không Nhận', 'A':'Đã Xác Nhận', 'R':'Từ Chối Giao Hàng', 'E':'Không Tiếp Tục Giao Hàng'}
        # get_ms_csv(DELI, 'DELI.csv')
        DELI=pd.read_csv('/home/biserver/data_lake/CSVDELI/DELI.csv', usecols=['BranchID', 'OrderNbr', 'Status'])
        DELI['BranchID_OrderNbr'] = DELI['BranchID']+DELI['OrderNbr']
        # DELI.columns
        DELI = DELI[['BranchID_OrderNbr','Status']]
        df2['trangthaigiaohang'] = (df2['BranchID']+df2['OrderNbr']).map(df_to_dict(DELI)).map(trangthaigiaohangdict)

        # end update 19/10
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
        df2['inserted_at'] = datetime.now()
        commit_psql("truncate table f_tracking_debt cascade;")
        execute_values_upsert(df2, 'f_tracking_debt', primary_keys)
        # rows = list(df2.itertuples(index=False, name=None))
        # hook_ps.insert_rows(table='f_tracking_debt', rows=rows, replace=True, target_fields=[x.lower() for x in df2.columns],replace_index=primary_keys)
    else: print("Not this time then")

dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_update_table = PythonOperator(task_id="update_table", python_callable=update_table, dag=dag)

py_etl_to_postgres = PythonOperator(task_id="etl_to_postgres", python_callable=etl_to_postgres, dag=dag)

tab_refresh = TableauRefreshWorkbookOperator(task_id='tab_refresh', workbook_name='Báo Cáo Doanh Thu Công Nợ', dag=dag)


dummy_start >> py_update_table >> py_etl_to_postgres >> tab_refresh
