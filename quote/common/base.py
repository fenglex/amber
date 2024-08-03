#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/8/3 17:29
# @Author  : haifeng
# @File    : common.py
from abc import ABC

from tushare.pro.client import DataApi

from quote.conf.config import Config
from quote.tools.db_tools import DuckDB
import tushare as ts


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


@singleton
class SdkBuilder:
    tushare_pro: DataApi

    def __init__(self):
        conf = Config()
        ts.set_token(conf.token)
        self.tushare_pro = self.tushare_pro = ts.pro_api()

    @property
    def get_tushare_pro(self):
        return self.tushare_pro


class Base(ABC):

    def __init__(self, table_name):
        self.conf = Config()
        self.table_name = table_name
        self.duck = DuckDB()

    def generator_table(self):
        pass

    def load_history_data(self):
        return self.duck.query(f"select  * from {self.table_name}")
