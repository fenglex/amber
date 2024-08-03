#!/usr/bin/python3
# setting
# -*- coding: utf-8 -*-
# @Time    : 2021/11/24 14:00
# @Author  : haifeng
from loguru import logger
from urllib.parse import quote_plus
import os


class Config:
    def __init__(self):
        self.token = os.getenv('TU_SHARE_TOKEN')
        self.db_file = "duck.db"
