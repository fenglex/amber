#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/8/6 17:09
# @Author  : haifeng
# @File    : base.py


class StockBase:

    def __init__(self):
        self.conf = Config()
        self.duck = DuckDB()
