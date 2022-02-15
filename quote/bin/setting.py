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
        self.token = 'e88ad9a2c34e9c77b6bd55d187694897fce2ab081f249c5cd8043cbb'
        self.url = 'mysql+pymysql://haifeng:X1PmkDrdjNsG@haifeng.ink:3306/db_quant?charset=utf8'
