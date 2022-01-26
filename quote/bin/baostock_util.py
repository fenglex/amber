#!/usr/bin/python3
# 
# -*- coding: utf-8 -*-
# @Time    : 2022/1/26 14:56
# @Author  : haifeng

import baostock as bs
import pandas as pd
import datetime


class BaoStockUtil:
    def __init__(self):
        bs.login()

    def basic_info(self):
        """
        获取证券基本资料
        :return:
        """
        rs = bs.query_stock_basic()
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        return pd.DataFrame(data_list, columns=['code', 'name', 'ipo_date', 'out_date', 'type', 'status'])

    def profit_data(self, code, start_year):
        """
        获取盈利能力
        :param code:
        :param start_year:
        :param end_year:
        :return:
        """
        current_year = datetime.datetime.now().year
        profit_list = []
        for year in range(start_year, current_year + 1):
            for quarter in range(1, 5):
                rs_profit = bs.query_profit_data(code=code, year=year, quarter=quarter)
