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
from bin.util import DbUtil
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s')


if __name__ == '__main__':
    setting = Setting()
    conn = create_engine(setting.url)
    connect = conn.connect()

    ts.set_token(setting.token)
    pro = ts.pro_api()
    db_util = DbUtil(conn)
    basic = Basic(pro)

    # db_util.to_mysql(basic.stock_list(), 'tb_stock_list', truncate_table=True)
    # db_util.to_mysql(basic.trade_calender(), 'tb_trade_calendar', truncate_table=True)
    db_util.to_mysql(basic.public_company_info(), 'tb_public_company_info', truncate_table=True)
    # basic.stock_list().to_excel('data.xlsx')
    # print(basic.stock_list())
