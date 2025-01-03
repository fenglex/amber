#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/27 14:31
# @Author  : haifeng
# @File    : quote.py

import easyquotation
import pandas as pd
from loguru import logger
import redis

from base_engine import MysqlStorageEngine

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'
engine = MysqlStorageEngine(host, port, user, password, 'db_quant')




ts_list = engine.query("SELECT ts_code,symbol FROM db_quant.tb_tushare_stock_list where market='主板'")
quotation = easyquotation.use('sina')
df = quotation.market_snapshot(prefix=False)
df = pd.DataFrame(df).T.reset_index(drop=False, names=['code'])
df = ts_list.merge(df, left_on='symbol', right_on='code', how='left')
df['change'] = (df['now'] - df['close']) / df['close'] * 100
print(df)
