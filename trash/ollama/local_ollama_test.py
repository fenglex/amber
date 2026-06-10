# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/21 10:51
# @Version: 1.0

import litellm
from langchain_litellm import ChatLiteLLM

litellm._turn_on_debug()
import os

os.environ['OLLAMA_API_BASE'] = "http://127.0.0.1:11434"

llm = ChatLiteLLM(model="qwen3:0.6b", custom_llm_provider="ollama", request_timeout=180)
response = llm.invoke(
    "中国2025年A股涨的可能性大吗？"
)
print(response)
