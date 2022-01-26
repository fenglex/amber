#!/usr/bin/python3
# 
# -*- coding: utf-8 -*-
# @Time    : 2022/1/26 14:36
# @Author  : haifeng


import baostock as bs
import pandas as pd

from quote.bin.setting import Setting
from quote.bin.util import DbUtil
from sqlalchemy import create_engine

setting = Setting()
conn = create_engine(setting.url)
connect = conn.connect()
db_util = DbUtil(conn)

# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 获取证券基本资料
# rs = bs.query_stock_basic(code="sh.600000")
rs = bs.query_stock_basic()
# rs = bs.query_stock_basic(code_name="浦发银行")  # 支持模糊查询
print('query_stock_basic respond error_code:' + rs.error_code)
print('query_stock_basic respond  error_msg:' + rs.error_msg)

# 打印结果集
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=['code', 'name', 'ipo_date', 'out_date', 'type', 'status'])

# 结果集输出到csv文件
db_util.to_mysql(result, 'tb_basic_info', truncate_table=True)
print(result)

# 登出系统
bs.logout()
