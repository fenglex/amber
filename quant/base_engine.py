#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/12 13:47
# @Author  : haifeng
# @File    : base_engine.py

import os
import urllib.parse
from datetime import datetime
from uuid import uuid4

import pandas as pd
import pymysql
from loguru import logger
import numpy as np


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
        # self.init()

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

    def __exec_sql(self, sql):
        with self.__get_conn() as conn:
            with conn.cursor() as cursor:
                logger.info("exec sql:\n" + sql)
                cursor.execute(sql)

    def __get_url(self):
        return f'mysql+pymysql://{self.user}:{urllib.parse.quote(self.password)}@{self.host}:{self.port}/{self.db_name}'

    def query(self, sql) -> pd.DataFrame:
        return pd.read_sql(sql, self.__get_url())

    def save_data(self, data_df: pd.DataFrame, table_name, batch_size: int = 50000):
        """
        :param upsert:
        :param data_df:
        :param table_name:
        :param truncate: 是否清空表
        :param batch_size: 批量插入大小,默认10000
        :return:
        """
        if data_df is None or len(data_df) == 0:
            return
        logger.info(f"save data to {table_name}, data size:{len(data_df)}")
        cols = data_df.columns.str.lower().tolist()
        if "id" not in cols:
            data_df['id'] = data_df.apply(lambda x: str(uuid4()), axis=1)
        if "update_time" not in cols:
            data_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_df = data_df.where(data_df.notnull(), None)
        cols = data_df.columns.str.lower().tolist()
        col_names = ','.join(cols)
        params = ','.join(['%s'] * len(cols))
        update_columns = ', '.join([f"{col}=VALUES({col})" for col in data_df.columns if col != 'id'])
        insert_sql = f"INSERT INTO {table_name} \n ({col_names}) VALUES ({params}) \n ON DUPLICATE KEY UPDATE \n {update_columns}"
        logger.info("insert sql:\n" + insert_sql)
        temp = []
        idx = 0
        with self.__get_conn() as conn:
            with conn.cursor() as cursor:
                for row in data_df.itertuples():
                    idx = idx + 1
                    temp.append(tuple(row[1:]))
                    if len(temp) >= batch_size or (idx == len(data_df) and len(temp) > 0):
                        cursor.executemany(insert_sql, temp)
                        logger.info(f"insert to:{table_name},{idx} records")
                        temp = []
            conn.commit()
