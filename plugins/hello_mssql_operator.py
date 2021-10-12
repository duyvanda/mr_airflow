from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

if TYPE_CHECKING:
    from airflow.hooks.dbapi import DbApiHook


class HelloMsSqlOperator(BaseOperator):
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
        self._hello()
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database, conn_type='odbc')
        result = hook.get_first(self.sql)
        print(result)
        return result

    def _hello(self):
        print("This is from hello")