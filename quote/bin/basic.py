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
            fields = 'ts_code,symbol,name,area,area,industry,fullname,enname,cnspell,market,exchange,curr_type,' \
                     'list_status,list_date,delist_date,is_hs'
        temp = []
        for status in ['L', 'D', 'P']:
            df = self.pro_api.stock_basic(exchange='', list_status=status, fields=fields)
            temp.append(df)
        return pd.concat(temp, axis=0)

    def trade_calender(self, start_date='', end_date='', exchange=['SSE', 'SZSE']):
        """
        获取交易日列表
        :return:
        """
        temp = []
        for exg in exchange:
            temp.append(self.pro_api.trade_cal(exchange=exg, start_date=start_date, end_date=end_date))
        return pd.concat(temp, axis=0)

    def public_company_info(self, exchange=['SSE', 'SZSE']):
        """
        上市公司信息
        :param ts_code: 股票代码
        :param exchange:
        :return:
        """
        temp = []
        fields = 'ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,' \
                 'website,email,office,employees,main_business,business_scope'
        for exg in exchange:
            temp.append(self.pro_api.stock_company(exchange=exg, fields=fields))
        return pd.concat(temp, axis=0)
