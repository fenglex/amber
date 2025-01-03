#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2025/1/2 17:17
# @Author  : haifeng
# @File    : backup_tables.py

from sqlalchemy import create_engine
import pandas as pd
from loguru import logger

host = '172.16.1.3'
port = 3306
user = 'quant'
password = '7$2%s2WLkU8!'

url = f'mysql+pymysql://{user}:{password}@{host}:{port}/db_quant'

# engine = create_engine(url)

df = pd.read_sql("show tables", con=url)
df.columns = ['table_name']
batch_size = 50000
for row in df.itertuples():
    cache = []
    id_temp = None
    record_count = 0
    tab = row.table_name
    while True:
        if id_temp is None:
            sql = f"select * from {tab} order by id limit {batch_size}"
        else:
            sql = f"select * from {tab} where id > '{id_temp}' order by id limit {batch_size} "
        logger.info(sql)
        df = pd.read_sql(sql, con=url)
        record_count += len(df)
        logger.info(f"load {record_count} records from {tab}")
        cache.append(df)
        if df.empty:
            break
        else:
            id_temp = df['id'].iloc[-1]
    data = pd.concat(cache)
    logger.info(f"{tab} has {len(data)} records")
    data.to_parquet(f"{tab}.parquet", index=False, compression='zstd')
