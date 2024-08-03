#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/8/3 16:40
# @Author  : haifeng
# @File    : db_tools.py
import duckdb
from quote.conf.config import Config


class DuckDB:

    def __init__(self):
        conf = Config()
        self.file = conf.db_file
        self.connect = duckdb.connect(self.file)

    def query(self, sql):
        return self.connect.query(sql).to_df()

    def insert_to(self, table, df):
        self.connect.execute(f"insert or replace into {table} select * from df")

    def execute(self, sql):
        self.connect.execute(sql)
