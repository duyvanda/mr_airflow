import os
import csv
import pandas as pd
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Union
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


if TYPE_CHECKING:
    from airflow.hooks.dbapi import DbApiHook
sql = "SELECT TOP 5 LUpd_Datetime as date, Level as marks, \
BranchID as name, BeforeVATPrice as bonus from OM_SalesOrdDet"

class InsertToPostgresOperator(BaseOperator):
    def __init__(
        self,
        *,
        sql: str,
        postgres_conn_id: str = 'postgres_default',
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.database = database

    def execute(self, context: dict) -> None:
        # df = self._get_data()
        df = self._get_df()
        rows = list(df.itertuples(index=False, name=None))
        hook = self._get_hook()
        hook.insert_rows(table='test_dataframe', rows=rows, replace=True, target_fields=[x.lower() for x in df.columns],replace_index=['marks','name'])


    def _get_data(self):
        startdate1=datetime(2021, 5, 1)
        startdate2=datetime(2021, 5, 2)
        startdate3=datetime(2021, 5, 3)
        data = {'date':[startdate1, startdate2, startdate3], 'marks':[5,6,7], 'name':['duy','khanh','vinh'],'bonus':[1.7, 2.7, 3.7]}
        df=pd.DataFrame(data)
        return df

    def _get_hook(self):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        return hook

    def _get_df(self):
        hook = MsSqlHook(mssql_conn_id='1_dms_conn_id', schema='PhaNam_eSales_PRO', conn_type='odbc')
        df_from_ms = hook.get_pandas_df(sql=sql)
        return df_from_ms



