#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/9/25 17:15
# @Author  : haifeng
# @File    : test.py

import duckdb

duck = duckdb.connect("20240925.db")
duck.sql("show tables").show()

