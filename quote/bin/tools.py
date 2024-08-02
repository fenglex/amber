#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/8/2 15:47
# @Author  : haifeng
# @File    : tools.py

from loguru import logger
from setting import Setting
import time
import pymysql


def format_seconds(seconds):
    """
    将秒数转换为小时、分钟和秒的格式
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    if seconds >= 3600:
        return f"{hours}时{minutes}分{seconds}秒"
    if minutes >= 60:
        return f"{minutes}分{seconds}秒"
    else:
        return f"{seconds}秒"


def generate_sql(cols, table, update_type: str = "insert"):
    if update_type == "insert":
        sql = f"""
            insert into {table} ({','.join(cols)})
            values ({','.join(list(map(lambda x: '%(' + x + ')s', cols)))})
        """
    elif update_type == "upsert":
        sql = f"""
            insert into {table} ({','.join(cols)})
            values ({','.join(list(map(lambda x: '%(' + x + ')s', cols)))})
            on duplicate key update
            {','.join([f'{col}=values({col})' for col in cols])}
        """
    else:
        raise Exception("update_key参数错误")
    return sql


def to_db(table, truncate=False, update_type='insert', batch_size=10000):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.info(f"开始更新{table}")
            start = int(time.time())
            df = func(*args, **kwargs)
            conf = Setting()
            with pymysql.connect(host=conf.db_host, user=conf.db_user, password=conf.db_password, database=conf.db_name,
                                 charset='utf8mb4') as con:
                sql = generate_sql(df.columns, table, update_type)
                with con.cursor() as cursor:
                    if truncate:
                        logger.info(f"清空表{table}")
                        cursor.execute(f"truncate table {table}")
                    logger.info(f"执行数据 sql:{sql}")
                    logger.info(f"总数据量{len(df)}")
                    for i in range(0, len(df), batch_size):
                        params = df.iloc[i:i + batch_size].apply(lambda x: dict(x), axis=1).values.tolist()
                        logger.info(f"当前进度:{i}~{i + len(params) - 1}")
                        cursor.executemany(sql, params)
                        con.commit()
            end = int(time.time())
            logger.info(f"更新{table}完成,耗时{format_seconds(end - start)},数据量{len(df)}")
            return df

        return wrapper

    return decorator
