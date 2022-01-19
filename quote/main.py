#!/usr/bin/python3
# 
# -*- coding: utf-8 -*-
# @Time    : 2022/1/19 18:46
# @Author  : haifeng

import tushare as ts
import pandas as pd
from bin.setting import Setting
from bin.basic import Basic
from sqlalchemy import create_engine

setting = Setting()
conn = create_engine(setting.url)
connect = conn.connect()

ts.set_token(setting.token)
pro = ts.pro_api()

basic = Basic(pro)
basic.stock_list().to_sql('tb_stock_list', con=conn, if_exists='replace', chunksize=5000, index=False)

# basic.stock_list().to_excel('data.xlsx')
# print(basic.stock_list())
