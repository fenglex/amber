#!/usr/bin/python3
# basic info
# -*- coding: utf-8 -*-
# @Time    : 2021/11/24 14:00
# @Author  : haifeng

import logging
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s')


class Basic:
    def __init__(self, pro_api):
        self.pro_api = pro_api

    def stock_list(self, fields=''):
        """
        股票列表
        :param fields:
        :return:
        """
        if len(fields) == 0:
            fields = 'ts_code,symbol,name,area,area,fullname,enname,cnspell,market,exchange,curr_type,list_status,' \
                     'list_date,delist_date,is_hs'
        temp = []
        for status in ['L', 'D', 'P']:
            df = self.pro_api.stock_basic(exchange='', list_status=status, fields=fields)
            temp.append(df)
        return pd.concat(temp, axis=0)

    def trade_calender(self, start_date):
        """
        获取交易日列表
        :return:
        """
        return self.pro_api.trade_cal(exchange='', start_date=start_date, end_date='')
