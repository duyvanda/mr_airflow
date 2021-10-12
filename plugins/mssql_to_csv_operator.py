import os
import csv
from datetime import datetime
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
        path: str = f'/home/biserver/data_lake/results.csv',
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.path = path
        self.sql = sql
        self.autocommit = autocommit
        self.database = database

    def execute(self, context: dict) -> None:
        data = self._get_data()
        cursor = self._get_cursor()
        with open(self.path, 'w', newline='', encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for row in data:
                writer.writerow([x[0] for x in cursor.description])
                writer.writerow(row)
        cursor.close

    def _get_data(self):
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database, conn_type='odbc')
        data = hook.get_records(self.sql)
        return data

    def _get_cursor(self):
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database, conn_type='odbc')
        cursor = hook.get_cursor()
        cursor.execute(self.sql)
        return cursor

