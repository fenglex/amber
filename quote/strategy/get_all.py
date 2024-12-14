#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @Time    : 2024/11/12 11:21
# @Author  : haifeng
# @File    : get_all.py
import adata

codes = adata.stock.info.all_code()
print(codes)
