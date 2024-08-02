#!/usr/bin/python3
# setting
# -*- coding: utf-8 -*-
# @Time    : 2021/11/24 14:00
# @Author  : haifeng
from loguru import logger
from urllib.parse import quote_plus


class Setting:
    def __init__(self):
        self.token = '78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348'
        self.db_name = 'db_quant'
        self.db_host = '172.16.1.3'
        self.db_user = 'quant'
        self.db_password = 'cJ5qXpDScH7@'
        self.db_port = 3306
