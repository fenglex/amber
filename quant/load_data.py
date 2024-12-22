#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/12 14:00
# @Author  : haifeng
# @File    : load_data.py

from datetime import datetime

import adata
import duckdb
import pandas as pd
from loguru import logger

duck_conn = duckdb.connect("duck.db")
from base_engine import MysqlStorageEngine

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'
engine = MysqlStorageEngine(host, port, user, password, 'db_quant')

if __name__ == '__main__':
    stock_df = adata.stock.info.all_code()
    # 同步个股列表
    file_path = "./data/tb_stock_list.parque"
    data_df = stock_df.rename(columns={'short_name': 'stock_name', "exchange": "market"})
    data_df.to_parquet(file_path, compression="zstd")
    # 同步个股收盘价
    table_name = "tb_stock_daily_price"
    file_path = "./data/tb_stock_daily_price.parque"
    sync_start_date = 20200101
    adjust_type = [0, 1, 2]
    all_data = []
    for tp in adjust_type:
        end_d = datetime.now().strftime('%Y-%m-%d')
        sql = f"select stock_code,adjust_type,max(trade_dt) as max_dt from '{file_path}' where adjust_type={tp}  group by adjust_type,stock_code"
        stock_history_df = engine.query(sql)
        all_code = stock_df.merge(stock_history_df, on='stock_code', how='left')
        all_code['max_dt'] = all_code['max_dt'].apply(lambda x: sync_start_date if pd.isna(x) else x)
        idx = 0
        for row in all_code.itertuples():
            try:
                idx = idx + 1
                code = row.stock_code
                start_d = int(row.max_dt)
                start_d = str(start_d)[:4] + "-" + str(start_d)[4:6] + "-" + str(start_d)[6:]
                data_df = adata.stock.market.get_market(code, start_date=start_d, end_date=end_d, adjust_type=tp)
                if len(data_df) > 0:
                    data_df.drop(columns=['trade_time'], inplace=True)
                    data_df['trade_dt'] = data_df['trade_dt'].astype(str)
                    data_df['trade_dt'] = data_df['trade_dt'].apply(lambda x: int(str(x).replace('-', '')))
                    data_df['adjust_type'] = tp
                    all_data.append(data_df)
                    break
                logger.info(f"获取{code},adjust_type:{tp},start_date:{start_d},数据量：{len(data_df)},进度:{idx}/{len(all_code)}")
            except Exception as e:
                logger.error(e)
        data_df = data_df.rename(
            columns={"trade_date": "trade_dt", "change": "chg", "change_pct": "chg_pct", "turnover_ratio": "turnover"})
    data_df = pd.concat(all_data)
    data_df.to_parquet(file_path, compression="zstd")
    # 同步同花顺概念

