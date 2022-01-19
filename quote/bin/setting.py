#!/usr/bin/python3
# setting
# -*- coding: utf-8 -*-
# @Time    : 2021/11/24 14:00
# @Author  : haifeng
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s')


class Setting:
    def __init__(self):
        self.token = 'tushare_toket'
        self.url = 'mysql+pymysql://user:password@host:port/database?charset=utf8'
