#!/bin/usr/env python3
# -*- coding: utf-8 -*-
# 
# author haifeng
# version: 1.0
# date 2022/01/19 21:31

import pandas as pd
import logging
import datetime

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 50)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s')


class DbUtil:
    def __init__(self, conn):
        self.conn = conn
        self.connect = self.conn.connect()

    def to_mysql(self, df, table, truncate_table=False):
        """
        写入mysql
        :param truncate_table: 是否清空表
        :param df: 原始数据
        :param table: 表名
        :return:
        """
        if len(df) != 0:
            logging.info('update {}'.format(table))
            if truncate_table:
                logging.info(' clear table {}'.format(table))
                self.connect.execute('truncate table {}'.format(table))
            df['create_time'] = datetime.datetime.now()
            df['update_time'] = datetime.datetime.now()
            df.insert(0, 'id', range(1, len(df) + 1))
            df.to_sql(table, con=self.conn, chunksize=5000, if_exists='append', index=False)
