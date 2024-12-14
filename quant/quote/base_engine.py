#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/12 13:47
# @Author  : haifeng
# @File    : base_engine.py

import pandas as pd
import os
import duckdb
from datetime import datetime
import pymysql
from loguru import logger
from typing import Union
import urllib.parse
from sqlalchemy.dialects.mysql import insert


class StorageEngine:
    def init(self):
        """
            初始化表结构
        """
        pass

    def save_data(self, data_df, table_name, upsert: bool = False):
        pass

    def query(self, sql) -> pd.DataFrame:
        pass


class MysqlStorageEngine(StorageEngine):
    """
    mysql存储引擎
    """

    def __init__(self, host, port, user, password, db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.init()

    def __get_conn(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               db=self.db_name,
                               charset='utf8mb4')

    def init(self):
        sql_file = os.sep.join(['sql', 'mysql.sql'])
        with open(sql_file, 'r', encoding='utf8', errors='ignore') as f:
            lines = f.readlines()

        # 去除空行和注释行 (假设注释以 -- 开头)
        filtered_lines = [line.strip() for line in lines if line.strip() and not line.startswith('--')]
        sqls = "\n".join(filtered_lines)
        sqls = sqls.split(";")
        sqls = [sql for sql in sqls if len(sql.strip()) > 10]
        with self.__get_conn() as conn:
            with conn.cursor() as cursor:
                for sql in sqls:
                    logger.info("exec sql:\n" + sql)
                    cursor.execute(sql)

    def save_data(self, data_df: pd.DataFrame, table_name, upsert: bool = False):
        """
        :param upsert:
        :param data_df:
        :param table_name:
        :return:
        """

        def sqlalchemy_upsert(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            for d in data:
                stat = insert(table.table).values(d).on_duplicate_key_update(
                    {key: d[key] for key in keys}
                )
                conn.execute(stat)

        engine = f'mysql+pymysql://{self.user}:{urllib.parse.quote(self.password)}@{self.host}:{self.port}/{self.db_name}'
        batch_size = 100
        if upsert:
            data_df.to_sql(table_name, engine, if_exists='append', method=sqlalchemy_upsert, index=False, chunksize=batch_size)
        else:
            data_df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=batch_size)
