# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/18 9:16
# @Version: 1.0


import pandas as pd
import talib
from akshare import fund_portfolio_hold_em
from rqalpha import run_func
from rqalpha.apis import history_bars, get_position, order_target_value, order_value, get_trading_dates, order_percent, \
    instruments, order_target_percent
from loguru import logger
import tushare as ts
from datetime import datetime
import os

token = os.getenv("TUSHARE_TOKEN")
print(token)
pro = ts.pro_api(token)

fund_portfolio_df = pro.fund_portfolio(ts_code="510880.SH")
fund_portfolio_df['symbol'] = fund_portfolio_df['symbol'].apply(lambda x: x.replace("SH", "XSHG"))
fund_portfolio_df['symbol'] = fund_portfolio_df['symbol'].apply(lambda x: x.replace("SZ", "XSHE"))


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    # 选择我们感兴趣的股票
    context.s1 = "510880.XSHE"
    # 获取每个月最后一个交易日
    trade_days = get_trading_dates("20100101", "20250718")
    trade_df = pd.DataFrame({"trade_day": trade_days})
    trade_df['trade_day'] = trade_df['trade_day'].apply(lambda x: x.strftime("%Y%m%d"))
    trade_df["month"] = trade_df['trade_day'].apply(lambda x: x[:6])
    trade_df = trade_df.drop_duplicates(subset=['month'], keep='last')
    month_last_days = trade_df['trade_day'].tolist()
    context.month_last_days = month_last_days
    context.SHORTPERIOD = 12
    context.LONGPERIOD = 26
    context.SMOOTHPERIOD = 9


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 开始编写你的主要的算法逻辑

    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合状态信息

    for pos in context.portfolio.positions:
        bars = history_bars(pos.order_book_id, 250, '1d',)
        df = pd.DataFrame(bars)
        df['rsi'] = talib.RSI(df['close'], timeperiod=14)
        cci=talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)
        if df['rsi'].iloc[-1] > 70:
            order_target_value(pos.order_book_id, 0)

    # 使用order_shares(id_or_ins, amount)方法进行落单
    cur_day = context.now.strftime("%Y%m%d")
    hold_df = fund_portfolio_df.copy()
    hold_df = hold_df[hold_df['end_date'] <= cur_day]
    max_end_date = hold_df['end_date'].max()
    hold_df = hold_df[hold_df['end_date'] == max_end_date]
    etf_hold = hold_df['symbol'].tolist()
    for etf_symbol in etf_hold:
        # 检查股票是否上市
        instrument = instruments(etf_symbol)
        if not instrument.listed:
            continue
        prices = history_bars(etf_symbol, 250, '1d', fields='close')
        # 用Talib计算MACD取值，得到三个时间序列数组，分别为macd, signal 和 hist
        macd, signal, hist = talib.MACD(prices, context.SHORTPERIOD,
                                        context.LONGPERIOD, context.SMOOTHPERIOD)

        # macd 是长短均线的差值，signal是macd的均线，使用macd策略有几种不同的方法，我们这里采用macd线突破signal线的判断方法

        # 如果macd从上往下跌破macd_signal

        if macd[-1] - signal[-1] < 0 and macd[-2] - signal[-2] > 0:
            # 获取当前投资组合中股票的仓位
            curPosition = get_position(etf_symbol).quantity
            # 进行清仓
            if curPosition > 0:
                order_target_value(etf_symbol, 0)

        # 如果短均线从下往上突破长均线，为入场信号 ，同时量柱大于0
        if macd[-1] - signal[-1] > 0 and macd[-2] - signal[-2] < 0:
            # 满仓入股
            # order_target_percent(etf_symbol, 0.1)
            order_target_value(etf_symbol, 20000)


config = {
    "base": {
        "start_date": "2014-01-01",
        "end_date": "2025-05-05",
        "benchmark": "000001.XSHG",
        "accounts": {
            "stock": 200000
        }
    },
    "extra": {
        "log_level": "info",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            "plot": True,
        },
    }
}

results = run_func(init=init, handle_bar=handle_bar, config=config)
print(results)
