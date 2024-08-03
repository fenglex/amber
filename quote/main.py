#!/usr/bin/python3
# 
# -*- coding: utf-8 -*-
# @Time    : 2022/1/19 18:46
# @Author  : haifeng

import tushare as ts
from basic import Basic
from prices import Prices
from quote.conf.config import Config


def calculate_macd(df, short_period=12, long_period=26, signal_period=9):
    """
    计算MACD指标

    Args:
        df: DataFrame，包含收盘价列
        short_period: 短期EMA周期
        long_period: 长期EMA周期
        signal_period: DEA的周期

    Returns:
        DataFrame，包含DIF、DEA、MACD柱
    """

    df['short_ema'] = df['close'].ewm(span=short_period, adjust=False).mean()
    df['long_ema'] = df['close'].ewm(span=long_period, adjust=False).mean()
    df['DIF'] = df['short_ema'] - df['long_ema']
    df['DEM'] = df['DIF'].ewm(span=signal_period, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEM'])

    return df


if __name__ == '__main__':
    setting = Config()
    ts.set_token(setting.token)
    pro = ts.pro_api()
    basic = Basic(pro)
    # basic.trade_calender()
    price = Prices(pro)
    df = price.stock_price('600900.SH', '20150101', '20240803')
    df = df.sort_values(by='trade_date')
    df = calculate_macd(df)
    print(df)
