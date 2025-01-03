#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/12/27 14:31
# @Author  : haifeng
# @File    : quote.py

import easyquotation
import pandas as pd
from loguru import logger
import redis



quotation = easyquotation.use('sina')
data = quotation.market_snapshot(prefix=False)
df = pd.DataFrame(data).T.reset_index(drop=False, names=['code'])
