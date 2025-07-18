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
    instruments
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


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    # 开始编写你的主要的算法逻辑

    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合状态信息

    # 使用order_shares(id_or_ins, amount)方法进行落单
    cur_day = context.now.strftime("%Y%m%d")

    for pos in context.portfolio.positions:
        instrument = instruments(pos)
        if not instrument.listed:
            continue
        bars = history_bars(pos, 250, '1d', fields=['close', 'open'])
        df = pd.DataFrame(bars)
        df['rsi'] = talib.RSI(df['close'], timeperiod=60)
        df['rsi_rank'] = df['rsi'].rank(ascending=True, pct=True)
        rsi_rank = df['rsi_rank'].iloc[-1]
        if rsi_rank >= 0.95:
            order_target_value(pos, 0)

    # 判断是否是当月最后一个交易日
    if cur_day not in context.month_last_days:
        return

    # 卖出所有
    for pos in context.portfolio.positions:
        order_target_value(pos, 0)

    hold_df = fund_portfolio_df.copy()
    hold_df = hold_df[hold_df['end_date'] <= cur_day]
    max_end_date = hold_df['end_date'].max()
    hold_df = hold_df[hold_df['end_date'] == max_end_date]
    etf_hold = hold_df['symbol'].tolist()
    rank = []
    for etf_symbol in etf_hold:
        # 检查股票是否上市
        instrument = instruments(etf_symbol)
        if not instrument.listed:
            continue
        bars = history_bars(etf_symbol, 250, '1d', fields=['close', 'open'])
        df = pd.DataFrame(bars)
        df['rsi'] = talib.RSI(df['close'], timeperiod=60)
        df['rsi_rank'] = df['rsi'].rank(ascending=True, pct=True)
        rank.append({"etf_symbol": etf_symbol, "rsi_rank": df['rsi_rank'].iloc[-1]})

    rank_df = pd.DataFrame(rank)
    rank_df = rank_df.sort_values(by='rsi_rank', ascending=True)
    rank_df = rank_df[rank_df['rsi_rank'] <= 0.1]
    rank_df['rsi_rank'] = rank_df['rsi_rank'].rank(ascending=True, pct=True)

    codes = rank_df.head(10)['etf_symbol'].to_list()
    for code in codes:
        order_percent(code, 1 / len(codes))
        logger.info(f"buy {code}")


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
