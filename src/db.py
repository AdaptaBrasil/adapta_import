# -*- coding: utf-8 -*-
import os
from sqlalchemy import create_engine, text
import pandas as pd
import pandas.io.sql as sqlio
import numpy as np

class dbThings:

    verbose = False
    conn_variable_name = None

    @classmethod
    def get_connection(cls):
        # AWS_ADAPTA_CONN_STR
        IMPORT_ADAPTA_CONN_STR = os.getenv(cls.conn_variable_name)
        engine = create_engine(IMPORT_ADAPTA_CONN_STR)
        return engine.connect()

    @classmethod
    def executeSQL(cls, sql: str, params: list = None):
        if params is not None:
            params = list(params)
            for i in range(len(params)):
                if type(params[i]) is np.int64:
                    params[i] = int(params[i])
        conn = cls.get_connection()
        print(f"Query: {sql}")
        if not params is None:
            print(f"Params: {params}")
        ret =  conn.execute( text(sql), params)
        conn.commit()
        conn.close()
        if cls.verbose:
            print(f"Row count: {ret.rowcount}")
        return ret

    @classmethod
    def getValue(cls, sql) -> object:
        ret = cls.executeSQL(sql).first()
        return None if len(ret) == 0 else ret[0]

    @classmethod
    def getSQLasDataframe(cls, sql: str) -> pd.DataFrame:
        conn = dbThings.get_connection()
        df = sqlio.read_sql_query(text(sql), conn)
        return df

    @classmethod
    def createIdImport(cls, schema: str, table_name : str):
        dbThings.executeSQL(f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS id_import int;")

