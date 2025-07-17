# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/17 17:21
# @Version: 1.0

import os

DEFAULT_CONFIG = {
    "DUCKDB_PATH": os.getenv("DUCKDB_PATH", "duckdb.db")
}
