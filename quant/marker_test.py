#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2025/1/3 14:18
# @Author  : haifeng
# @File    : marker_test.py
import pandas as pd

from base_engine import MysqlStorageEngine

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'
engine = MysqlStorageEngine(host, port, user, password, 'db_quant')

ts_list = engine.query("SELECT ts_code,symbol FROM db_quant.tb_tushare_stock_list where market='主板'")

file = "C:\\Users\\fengl\\Desktop\\tb_stock_daily_price.parquet"

df = pd.read_parquet(file)
df = ts_list.merge(df, left_on='symbol', right_on='stock_code', how='left')
df = df[df['chg_pct'] < 11]
df = df.sort_values('trade_dt')
df['pre_change'] = df.groupby('stock_code')['chg_pct'].shift(1)
df['pre2_change'] = df.groupby('stock_code')['chg_pct'].shift(2)
# 前一日涨停的数量
df = df[(df['pre_change'] >= 9.9) & (df['pre_change'] <= 10.5) & (df['pre2_change'] <= 9.5)]

print(len(df))
# 当日涨停的数量
# 前一日涨停,当日上涨的数量
dd = df[df['chg_pct'] >= 1]
print(len(dd))
print(1)
