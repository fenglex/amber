#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/12 14:00
# @Author  : haifeng
# @File    : load_data.py
from uuid import uuid4

import akshare as ak
import pandas as pd
import duckdb
from loguru import logger
from datetime import datetime

duck_conn = duckdb.connect("duck.db")
from base_engine import MysqlStorageEngine

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'
engine = MysqlStorageEngine(host, port, user, password, 'db_quant')

rename = {"代码": "stock_code", "名称": "stock_name"}
df = ak.stock_zh_a_spot_em()
df = df.rename(columns=rename)[['stock_code', 'stock_name']]

rename = {"日期": "trade_dt",
          "股票代码": "stock_code",
          "开盘": "open",
          "收盘": "close",
          "最高": "high",
          "最低": "low",
          "成交量": "volume",
          "成交额": "amount",
          "振幅": "amplitude",
          "涨跌幅": "chg",
          "涨跌额": "chg_pct",
          "换手率": "turnover"}
idx = 0
datas = []
for row in df.itertuples():
    stock_code = row.stock_code
    try:
        stock_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date="20170301", end_date='20241212', adjust="")
        stock_df = stock_df.rename(columns=rename)
        datas.append(stock_df)
    except Exception as e:
        logger.error(e)
    idx = idx + 1
    logger.info(f"{idx}/{len(df)} {stock_code}")
df = pd.concat(datas)
df['trade_dt'] = df['trade_dt'].astype('str').str.replace("-", "")
df['id'] = df.apply(lambda x: str(uuid4()), axis=1)
df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"total data: {len(df)}")
engine.save_data(df, "tb_stock_eod_price", upsert=True)
df.to_parquet("stock_eod_price.parquet")
