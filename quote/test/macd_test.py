#!/usr/bin/env python
# -*- coding: utf-8 -*-
# macd交叉线产生后，持有两天收益
# @Time    : 2024/8/4 10:00
# @Author  : haifeng
# @File    : macd_test.py

import pandas as pd

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


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
    df = pd.read_parquet('D:\\workspace\\amber\\quote\\stock_price.parquet')
    df = df[['ts_code', 'trade_date', 'close', 'pre_close']]
    df = df.sort_values(by=['trade_date'])
    # df = df[df['ts_code'] == '600900.SH']
    df = df.groupby('ts_code')
    datas = []
    for group, g_df in df:
        data = calculate_macd(g_df)
        data['MACD_shift1'] = data['MACD'].shift(1)
        data['MACD_shift2'] = data['MACD'].shift(2)
        data['last'] = data.apply(
            lambda x: (x['MACD'] >= 0 and x['MACD_shift1'] < 0) | (x['MACD'] >= 0 and x['MACD_shift2'] < 0), axis=1)
        data = data[data['trade_date'] >= '20240802'].reset_index(drop=True)
        if len(data) > 0 and data.loc[0, 'last'] and data.loc[0, 'DIF'] > 0:
            datas.append(data)
    print(pd.concat(datas).reset_index(drop=True))
