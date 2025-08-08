#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import zmq
import time
import threading
import argparse
import sys
import json
import os
from loguru import logger
from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass, asdict
import easyquotation
from datetime import datetime


def setup_logging():
    """设置日志记录"""
    logger.remove()
    # 确保日志目录存在
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logger.add(
        "logs/zmq_server.log",
        rotation="10 MB",
        retention="10 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    logger.add(
        sys.stdout,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    logger.info("ZeroMQ服务端日志系统初始化完成")


@dataclass
class Quote:
    code: str
    name: str
    price: float
    open: float
    high: float
    low: float
    pre_close: float
    volume: float  # 股票成交量
    amount: float  # 股票成交额
    change: float  # 涨跌幅 %
    buy: List[float]  # 买一价，买二价，买三价，买四价，买五价
    buy_volume: List[int]  # 买一量，买二量，买三量，买四量，买五量
    sell: List[float]  # 卖一价，卖二价，卖三价，卖四价，卖五价
    sell_volume: List[int]  # 卖一量，卖二量，卖三量，卖四量，卖五量
    date: int
    time: int
    current_time: str


class Quotation:

    def get_quotation(self, drop_duplicate=True) -> List[Quote]:
        pass


class QuotationSina(Quotation):
    def __init__(self):
        self.quotation = easyquotation.use('sina')
        self.cache = {}

    @staticmethod
    def __to_ts_code(code):
        return f"{code[2:]}.{code[0:2]}".upper()

    def get_quotation(self, drop_duplicate=True):
        tmp = self.quotation.market_snapshot(prefix=True)
        result = []
        for k, v in tmp.items():
            code = self.__to_ts_code(k)
            q = Quote(
                code=code,
                name=v['name'],
                price=v['now'],
                open=v['open'],
                high=v['high'],
                low=v['low'],
                pre_close=v['close'],
                volume=v['turnover'],
                amount=v['volume'],
                change=None if v['close'] == 0 else (v['now'] - v['close']) / v['close'],
                buy=[v['bid1'], v['bid2'], v['bid3'], v['bid4'], v['bid5']],
                buy_volume=[v['bid1_volume'], v['bid2_volume'], v['bid3_volume'], v['bid4_volume'], v['bid5_volume']],
                sell=[v['ask1'], v['ask2'], v['ask3'], v['ask4'], v['ask5']],
                sell_volume=[v['ask1_volume'], v['ask2_volume'], v['ask3_volume'], v['ask4_volume'], v['ask5_volume']],
                date=int(v['date'].replace('-', '')),
                time=int(v['time'].replace(':', '')),
                current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            )
            if drop_duplicate and code in self.cache:
                cache_tmp = self.cache[code]
                if q.date >= cache_tmp.date and q.time > cache_tmp.time:
                    result.append(q)
            else:
                result.append(q)
            self.cache[code] = q
        return result


class Authenticator(ABC):
    """认证器抽象基类"""

    @abstractmethod
    def authenticate(self, username, password):
        """认证方法"""
        pass


class DefaultAuthenticator(Authenticator):
    """默认认证器"""

    def __init__(self):
        self.users = {
            "admin": "wfNL26nq",
            "haifeng": "MY8pC6wO",
            "hadoop": "04mNubXy",
            "flink": "0ZQxXgWN"
        }

    def authenticate(self, username, password):
        """默认认证方法，允许所有用户访问"""
        if username in self.users and self.users[username] == password:
            logger.info(f"用户认证成功: {username}")
            return True
        else:
            logger.warning(f"用户认证失败: {username}")
            return False


class ZMQServer:
    """ZeroMQ服务端类"""

    def __init__(self, quotation: Quotation, port=5555, interval=3):
        self.port = port
        self.context = zmq.Context()
        self.authenticator = DefaultAuthenticator()
        self.running = False
        self.authenticated_clients = set()  # 存储已认证的客户端
        self.interval = interval

        # 生成服务器密钥对
        self.server_keys = zmq.curve_keypair()
        self.quote: Quotation = quotation
        self.separator = "\001"
        self.counter = 0

    def sender_counter(self, counter=1):
        self.counter = self.counter + counter
        if self.counter % 100000 == 0:
            logger.info(f"已发送: {self.counter} 条消息")

    def send_message(self, topic, message):
        """发送消息"""
        self.publisher.send_string(f"{topic}{self.separator}{message}")
        self.sender_counter()

    def start(self):
        """启动服务端"""
        try:
            # 创建PUB套接字
            self.publisher = self.context.socket(zmq.PUB)
            # 设置安全机制
            self._setup_security()
            self.publisher.bind(f"tcp://*:{self.port}")
            logger.info(f"ZeroMQ服务端已在端口 {self.port} 启动")
            # 启动认证服务
            self._setup_auth()
            self.running = True
            # 发布示例消息
            self._publish_messages()

        except Exception as e:
            logger.error(f"服务端启动失败: {e}")
        finally:
            self.stop()

    def _setup_security(self):
        """设置安全机制"""
        self.publisher.setsockopt(zmq.CURVE_SERVER, 1)
        self.publisher.setsockopt_string(zmq.CURVE_SECRETKEY, self.server_keys[1].decode('utf-8'))
        logger.info("已设置服务器安全机制")

    def _setup_auth(self):
        """设置认证"""
        # 启动认证线程
        self.auth_thread = threading.Thread(target=self._handle_auth, daemon=True)
        self.auth_thread.start()

    def _handle_auth(self):
        """处理认证请求"""
        auth_socket = self.context.socket(zmq.REP)
        auth_socket.bind("tcp://*:5556")

        logger.info("认证服务监听在端口 5556")

        while self.running:
            try:
                # 等待认证请求
                message = auth_socket.recv_string()
                data = json.loads(message)
                username = data.get("username")
                password = data.get("password")
                # 执行认证
                if self.authenticator.authenticate(username, password):
                    # 认证成功时返回服务器公钥
                    response = {
                        "status": "success",
                        "message": "认证成功",
                        "server_key": self.server_keys[0].decode('utf-8')
                    }
                    auth_socket.send_string(json.dumps(response))
                else:
                    response = {"status": "error", "message": "认证失败"}
                    auth_socket.send_string(json.dumps(response))

            except Exception as e:
                logger.error(f"认证处理出错: {e}")
                response = {"status": "error", "message": "服务器内部错误"}
                try:
                    auth_socket.send_string(json.dumps(response))
                except:
                    pass

    def _publish_messages(self):
        """发布消息"""
        logger.info("开始发布消息")
        while self.running:
            # 如果是15:30~:8:00,sleep10分钟，否则正常往下走
            int_time = int(datetime.now().strftime("%H%M"))
            if int_time >= 1530 or int_time < 900:
                logger.info(f"当前时间是{datetime.now().strftime('%H:%M')}，休眠10分钟")
                time.sleep(600)
                continue
            try:
                datas = self.quote.get_quotation(drop_duplicate=True)
                topic = 'ticker'
                logger.info(f"获取{len(datas)}条{topic}数据")
                if len(datas) > 0:
                    # 发送每条数据
                    for data in datas:
                        self.send_message(topic, json.dumps(asdict(data)))
                time.sleep(self.interval)
            except KeyboardInterrupt:
                logger.info("收到中断信号，停止发布消息")
                break
            except Exception as e:
                logger.error(f"发布消息时出错: {e}")

    def stop(self):
        """停止服务端"""
        self.running = False
        if hasattr(self, 'publisher'):
            self.publisher.close()
        self.context.term()
        logger.info("ZeroMQ服务端已停止")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="ZeroMQ服务端")
    parser.add_argument("--port", type=int, default=5555, help="发布端口 (默认: 5555)")
    args = parser.parse_args()
    # 设置日志
    setup_logging()
    # 创建并启动服务端
    quotation = QuotationSina()
    server = ZMQServer(quotation, port=args.port, interval=30)
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭服务端...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
