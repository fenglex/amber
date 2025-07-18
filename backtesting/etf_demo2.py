# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/18 10:42
# @Version: 1.0

# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/18 15:30（优化版）
# @Version: 2.0
import pandas as pd
import numpy as np
import talib
from rqalpha import run_func
from rqalpha.apis import history_bars, get_position, order_target_value, order_percent, get_trading_dates, instruments
from loguru import logger
import tushare as ts
from datetime import datetime
import os

token = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(token)

# 初始化基础参数
RSI_PERIOD = 60  # RSI周期
CCI_PERIOD = 14  # CCI周期
ATR_PERIOD = 14  # ATR周期
RSI_THRESHOLD = 30  # RSI超卖阈值
CCI_THRESHOLD = -100  # CCI超卖阈值
VOLATILITY_WINDOW = 20  # 波动率计算窗口
DOWNTREND_WINDOW = 20  # 下跌趋势确认窗口

fund_portfolio_df = pro.fund_portfolio(ts_code="510880.SH")
fund_portfolio_df['symbol'] = fund_portfolio_df['symbol'].apply(lambda x: x.replace("SH", "XSHG"))
fund_portfolio_df['symbol'] = fund_portfolio_df['symbol'].apply(lambda x: x.replace("SZ", "XSHE"))


def init(context):
    context.s1 = "510880.XSHE"  # 基准ETF
    # 计算每月最后一个交易日（用于调仓）
    trade_days = get_trading_dates("20100101", "20250718")
    trade_df = pd.DataFrame({"trade_day": trade_days})
    trade_df['trade_day'] = trade_df['trade_day'].apply(lambda x: x.strftime("%Y%m%d"))
    trade_df['month'] = trade_df['trade_day'].apply(lambda x: x[:6])
    context.month_last_days = trade_df.groupby('month')['trade_day'].last().tolist()

    # 初始化参数校验
    logger.info(f"策略初始化完成，参数：RSI={RSI_PERIOD}, CCI={CCI_PERIOD}, ATR={ATR_PERIOD}")


def handle_bar(context, bar_dict):
    cur_day = context.now.strftime("%Y%m%d")

    # 非调仓日：持有检查与动态止盈
    if cur_day not in context.month_last_days:
        check_positions(context)
        return

    # 调仓日：清仓+重新选基
    logger.info(f"调仓日：{cur_day}，执行低吸策略")
    liquidate_all(context)  # 清空现有持仓

    # 获取当前可投标的（示例为沪深300成分股，可根据需求调整）
    target_etfs = get_target_etfs(context)
    if not target_etfs:
        logger.warning("无可满足条件的ETF，今日不操作")
        return

    # 多指标筛选低吸标的
    selected_etfs = multi_factor_screening(context, target_etfs)
    if not selected_etfs:
        logger.warning("筛选后无符合条件的ETF，今日不操作")
        return

    # 仓位分配（等权重）
    allocate_positions(context, selected_etfs)


def check_positions(context):
    """持仓检查：动态止盈/止损"""
    for pos in context.portfolio.positions:
        instrument = instruments(pos)
        if not instrument.listed:
            continue

        # 获取近期数据
        bars = history_bars(pos, 20, '1d', fields=['close', 'high', 'low'])
        df = pd.DataFrame(bars)

        # 计算关键指标
        current_rsi = talib.RSI(df['close'], RSI_PERIOD).iloc[-1]
        current_cci = talib.CCI(df['high'], df['low'], df['close'], CCI_PERIOD).iloc[-1]
        atr = talib.ATR(df['high'], df['low'], df['close'], ATR_PERIOD).iloc[-1]

        # 止盈条件：RSI超买（>70）且盈利5%以上
        if current_rsi > 70 and context.portfolio.positions[pos].total_value * 0.05 < (
                df['close'].iloc[-1] - df['close'].iloc[-20:].mean()) * pos.quantity:
            order_target_value(pos, 0)
            logger.info(f"{pos} RSI超买止盈，卖出")

        # 止损条件：CCI严重超卖（<-200）或ATR异常放大（>2倍均值）
        df['atr'] = talib.ATR(df['high'], df['low'], df['close'], ATR_PERIOD)
        avg_atr = df['atr'].iloc[-20:].mean()
        if current_cci < -200 or (atr > 2 * avg_atr):
            order_target_value(pos, 0)
            logger.info(f"{pos} 触发止损条件，卖出")


def liquidate_all(context):
    """清空所有持仓"""
    for pos in list(context.portfolio.positions.values()):
        order_target_value(pos.order_book_id, 0)
    logger.info("已完成全部持仓清空")


def get_target_etfs(context):
    """获取候选ETF池（示例为当前持有的ETF，可扩展为全市场筛选）"""
    cur_day = context.now.strftime("%Y%m%d")
    hold_df = fund_portfolio_df.copy()
    hold_df = hold_df[hold_df['end_date'] <= cur_day]
    max_end_date = hold_df['end_date'].max()
    return hold_df[hold_df['end_date'] == max_end_date]['symbol'].tolist()


def multi_factor_screening(context, etf_list):
    """多指标融合筛选低吸标的"""
    candidate = []

    for etf in etf_list:
        # 数据获取（包含高低开收）
        instrument = instruments(etf)
        if not instrument.listed:
            continue
        bars = history_bars(etf, 50, '1d', fields=['close', 'high', 'low', 'open'])
        if len(bars) < 50:
            logger.warning(f"{etf} 数据不足，跳过")
            continue
        df = pd.DataFrame(bars)

        # 指标计算
        rsi = talib.RSI(df['close'], RSI_PERIOD).iloc[-1]
        cci = talib.CCI(df['high'], df['low'], df['close'], CCI_PERIOD).iloc[-1]
        atr = talib.ATR(df['high'], df['low'], df['close'], ATR_PERIOD).iloc[-1]

        # 趋势确认：收盘价低于20日均线（下跌趋势中）
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        price_below_ma = df['close'].iloc[-1] < ma20

        # 波动率过滤：当前ATR低于过去20天平均（波动收敛）
        df['atr'] = talib.ATR(df['high'], df['low'], df['close'], ATR_PERIOD)
        avg_atr = df['atr'].iloc[-VOLATILITY_WINDOW:].mean()
        volatility_converge = atr < avg_atr

        # 超卖确认：RSI<30且CCI<-100
        oversold = (rsi < RSI_THRESHOLD) & (cci < CCI_THRESHOLD)

        # 综合评分（可扩展为加权评分）
        score = sum([oversold, volatility_converge, price_below_ma])

        candidate.append({
            "symbol": etf,
            "rsi": rsi,
            "cci": cci,
            "atr": atr,
            "score": score,
            "ma20": ma20
        })

    if not candidate:
        return []

    # 按综合评分排序（得分越高越符合条件）
    df = pd.DataFrame(candidate).sort_values('score', ascending=False)
    return df.head(15)['symbol'].tolist()  # 取前15个候选


def allocate_positions(context, etf_list):
    """仓位分配（等权重）"""
    if not etf_list:
        return

    weight = 1.0 / len(etf_list)
    for etf in etf_list:
        order_percent(etf, weight)
        logger.info(f"买入 {etf}，仓位占比：{weight:.2%}")


# 策略配置
config = {
    "base": {
        "start_date": "2018-01-01",  # 测试起始时间
        "end_date": "2025-05-05",  # 测试结束时间
        "benchmark": "000300.XSHG",  # 对标沪深300
        "accounts": {"stock": 200000},  # 初始资金20万
        "frequency": "1d"  # 日线回测
    },
    "extra": {
        "log_level": "info",
        "init_position_ratio": 0.95  # 初始仓位比例
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            "plot": True,
            "report_save_path": "./report"  # 生成报告路径
        }
    }
}

# 执行回测
results = run_func(init=init, handle_bar=handle_bar, config=config)
print(f"回测完成，最终净值")
