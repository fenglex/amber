#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/12 14:00
# @Author  : haifeng
# @File    : load_data.py

from datetime import datetime

import adata
import duckdb
import os
import pandas as pd
from loguru import logger
from base_engine import MysqlStorageEngine
import tushare as ts

duck_engine = duckdb.connect()

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'
engine = MysqlStorageEngine(host, port, user, password, 'db_quant')


def sync_data(start_date):
    stock_df = adata.stock.info.all_code()
    # 同步个股列表
    data_df = stock_df.rename(columns={'short_name': 'stock_name', "exchange": "market"})
    engine.save_data(data_df, 'tb_stock_list')
    # 同步个股收盘价
    table_name = 'tb_stock_daily_price'
    adjust_type = [0]
    all_data = []
    for tp in adjust_type:
        end_d = datetime.now().strftime('%Y-%m-%d')
        sql = f"select stock_code,max(trade_dt) as max_dt from {table_name} where adjust_type = {tp} group by stock_code"
        his_df = engine.query(sql)
        stock_df = stock_df.merge(his_df, on='stock_code', how='left')
        stock_df['start_dt'] = stock_df.apply(lambda x: str(start_date) if pd.isna(x.max_dt) else x.max_dt, axis=1)
        idx = 0
        for row in stock_df.itertuples():
            try:
                idx = idx + 1
                code = row.stock_code
                start_d = int(row.start_dt)
                start_d = str(start_d)[:4] + "-" + str(start_d)[4:6] + "-" + str(start_d)[6:]
                data_df = adata.stock.market.get_market(code, start_date=start_d, end_date=end_d, adjust_type=tp)
                if len(data_df) > 0:
                    data_df = data_df.rename(
                        columns={"trade_date": "trade_dt", "change": "chg", "change_pct": "chg_pct", "turnover_ratio": "turnover"})
                    data_df.drop(columns=['trade_time'], inplace=True)
                    data_df['trade_dt'] = data_df['trade_dt'].astype(str)
                    data_df['trade_dt'] = data_df['trade_dt'].apply(lambda x: int(str(x).replace('-', '')))
                    data_df['adjust_type'] = tp
                    all_data.append(data_df)
                logger.info(f"获取{code},adjust_type:{tp},start_date:{start_d},数据量：{len(data_df)},进度:{idx}/{len(stock_df)}")
            except Exception as e:
                logger.error(e)
    engine.save_data(pd.concat(all_data), table_name)
    # 同步同花顺概念


if __name__ == '__main__':
    sync_data('20210101')
