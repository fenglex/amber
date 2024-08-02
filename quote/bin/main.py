#!/usr/bin/python3
# 
# -*- coding: utf-8 -*-
# @Time    : 2022/1/19 18:46
# @Author  : haifeng

from loguru import logger
import tushare as ts
from basic import Basic
from setting import Setting

if __name__ == '__main__':
    setting = Setting()
    ts.set_token(setting.token)
    pro = ts.pro_api()
    basic = Basic(pro)
    basic.trade_calender()
