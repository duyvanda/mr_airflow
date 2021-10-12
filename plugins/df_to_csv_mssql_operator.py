import os
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Union
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

if TYPE_CHECKING:
    from airflow.hooks.dbapi import DbApiHook


class ToCSVMsSqlOperator(BaseOperator):
    def __init__(
        self,
        *,
        sql: str,
        mssql_conn_id: str = 'mssql_default',
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.database = database

    def execute(self, context: dict) -> None:
        df = self._get_df()
        print(df.shape)
        path = os.getcwd()+"/data/"
        df.to_csv(path+"raw_data_result.csv", index=False)

    def _get_df(self):
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database, conn_type='odbc')
        df = hook.get_pandas_df(self.sql)
        return df
