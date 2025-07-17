# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/17 15:55
# @Version: 1.0
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/17 9:22
# @Version: 1.0
import os

from langchain_core.callbacks import CallbackManager, StreamingStdOutCallbackHandler
from langchain_core.messages import AIMessage
from langgraph.prebuilt import create_react_agent
import akshare as ak
import pandas as pd
from langchain_litellm import ChatLiteLLM
import json
import talib as ta
import tushare as ts
from cachetools import cached, TTLCache




class FundDataProvider:
    cache = TTLCache(maxsize=500, ttl=60 * 60)
    def __init__(self):
        tushare_token = os.getenv("TUSHARE_TOKEN")
        self.pro = ts.pro_api(token=tushare_token)

    @cached(cache)
    def get_etf_components(self, etf_code,cur_date):
        """
        获取公募基金和ETF成分股
        :param etf_code: etf代码
        :param cur_date: 当前日期,格式YYYYMMDD
        :return:
        """
        #检查日期格式

        data = self.pro.etf_portfolio(ts_code=etf_code)
        return data


# 获取etf的成分股


def get_stock_price_data(stock_code, start_date, end_date, adjust=None) -> str:
    """
    从数据库中获取股票价格数据
    :param stock_code: 股票代码
    :param start_date: 开始日期,格式YYYYMMDD
    :param end_date: 结束日期,格式YYYYMMDD
    :param adjust: 调整方式，不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据
    :return: 股票价格数据
    """
    data = ak.stock_zh_a_hist(stock_code, period="daily", start_date=start_date, end_date=end_date, adjust=adjust)
    return data.to_csv(index=False)


llm = ChatLiteLLM(
    model="qwen-plus",
    custom_llm_provider="openai",
    api_key="sk-d14b062742cd",
    api_base="https://dashscope.aliyuncs.com/compatible-mode/v1",
    # llm_provider="ollama",     # 关键点，指定 ollama
    streaming=False,
    verbose=True,
    callback_manager=CallbackManager([StreamingStdOutCallbackHandler()])

)

agent = create_react_agent(
    model=llm,
    tools=[get_stock_price_data],
    prompt="你是一个股票分析助手",
    debug=True
)
prompt = """
当前的日期是20250707，帮我获取600519 近一年的价格数据
然后帮我计算KDJ,CCI 指标,**必须给出所有日期的具体值**
"""
# Run the agent
result = agent.invoke(
    {"messages": [{"role": "user", "content": prompt}]}
)

print(result)
message: AIMessage = result['messages'][-1]

print(result['messages'][-1].content)
