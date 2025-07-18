# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/17 16:13
# @Version: 1.0

import pandas as pd
from functools import wraps
from abc import ABC, abstractmethod
from default_config import DEFAULT_CONFIG
import duckdb


class StorageBase(ABC):
    @abstractmethod
    def create_table(self, table: str):
        """
        创建表
        """
        pass

    @abstractmethod
    def save(self, table: str, data: pd.DataFrame):
        """
        保存数据到指定的表
        Args:
            table: 表名
            data: 数据
        """
        pass


class DuckDBStorage(StorageBase):
    def __init__(self):
        self.db_path = DEFAULT_CONFIG.get("DUCKDB_PATH")

    def create_table(self, table: str):
        pass

    def save(self, table: str, data: pd.DataFrame):
        pass


class StorageFactory:
    _storage_map = {
        'duckdb': DuckDBStorage()
    }

    @classmethod
    def get_storage(cls, backend='duckdb') -> StorageBase:
        assert backend in cls._storage_map.keys(), f"Unknown backend: {backend}"
        return cls._storage_map[backend.lower()]


# 装饰器类
class Storage:
    _default_storage = "duckdb"

    def __init__(self, table, replace=False, backend="duckdb"):
        """
        初始化存储类
        Args:
            table: 表名
            replace: 是否替换已存在的表
            backend: 存储后端，支持duckdb
        """
        self.table = table
        self.replace = replace
        self.storage = StorageFactory.get_storage(backend)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if not isinstance(result, pd.DataFrame):
                raise ValueError("Function must return a pandas DataFrame")
            if len(result) > 0:
                # 确定使用的存储实例
                storage: StorageBase = self.storage
                # 创建表
                storage.create_table(self.table)
                # 保存数据
                storage.save(self.table, result)
            return result

        return wrapper


if __name__ == '__main__':
    # 生成各个类型的pandas的数据类型的dataframe
    pd_df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
        'date_col': pd.date_range('2025-01-01', periods=3),
        'datetime_col': pd.date_range('2025-01-01 00:00:00', periods=3),
    })
    cols = pd_df.dtypes
    types = []
    print(duckdb.typing.DuckDBPyType("str"))