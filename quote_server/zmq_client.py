#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import zmq
import time
import argparse
import sys
import json
import threading
import os
from loguru import logger
from abc import ABC, abstractmethod


def setup_logging():
    """设置日志记录"""
    logger.remove()
    logger.add(
        "logs/zmq_client.log",
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
    logger.info("ZeroMQ客户端日志系统初始化完成")


class Authenticator(ABC):
    """认证器抽象基类"""

    @abstractmethod
    def authenticate(self, username, password, address):
        """认证方法"""
        pass


class ZMQAuthenticator(Authenticator):
    """ZeroMQ认证器"""

    def __init__(self, context):
        self.context = context

    def authenticate(self, username, password, address):
        """通过ZeroMQ进行认证"""
        try:
            # 创建请求套接字
            auth_socket = self.context.socket(zmq.REQ)
            auth_socket.connect(f"tcp://{address}:5556")

            # 发送认证请求
            auth_data = {
                "username": username,
                "password": password
            }
            auth_socket.send_string(json.dumps(auth_data))

            # 等待响应
            message = auth_socket.recv_string()
            response = json.loads(message)

            auth_socket.close()

            if response.get("status") == "success":
                logger.info(f"客户端认证成功: {username}")
                return response.get("server_key")
            else:
                logger.error(f"客户端认证失败: {username} - {response.get('message')}")
                return None

        except Exception as e:
            logger.error(f"认证过程中出错: {e}")
            return None


class ZMQClient:
    """ZeroMQ客户端类"""

    def __init__(self, username, password, server_address="localhost", port=5555):
        self.server_address = server_address
        self.port = port
        self.username = username
        self.password = password
        self.context = zmq.Context()
        self.authenticator = ZMQAuthenticator(self.context)
        self.subscriber = None
        self.running = False
        self.authenticated = False
        self.subscribed_topics = set()  # 记录已订阅的主题
        self.message_handlers = {}  # 存储不同主题的消息处理函数
        self.separator = "\001"

    def connect(self):
        """连接到服务端"""
        try:
            # 先进行认证
            server_key = self.authenticator.authenticate(self.username, self.password, self.server_address)
            if not server_key:
                logger.error("认证失败，无法连接到服务端")
                return False

            # 生成客户端密钥对
            client_keys = zmq.curve_keypair()

            # 创建SUB套接字
            self.subscriber = self.context.socket(zmq.SUB)

            # 设置安全机制
            self.subscriber.setsockopt(zmq.CURVE_SERVERKEY, server_key.encode('utf-8'))
            self.subscriber.setsockopt(zmq.CURVE_PUBLICKEY, client_keys[0])
            self.subscriber.setsockopt(zmq.CURVE_SECRETKEY, client_keys[1])
            self.subscriber.connect(f"tcp://{self.server_address}:{self.port}")
            self.authenticated = True
            logger.info(f"成功连接到服务端 {self.server_address}:{self.port}")
            return True

        except Exception as e:
            logger.error(f"连接服务端失败: {e}")
            return False

    def register_message_handler(self, topic, handler):
        """
        注册特定主题的消息处理函数
        
        Args:
            topic (str): 主题名称
            handler (function): 处理该主题消息的函数
        """
        self.message_handlers[topic] = handler
        logger.info(f"已注册主题 '{topic}' 的消息处理函数")

    def _parse_message(self, message):
        """
        解析接收到的消息，提取主题和内容
        
        Args:
            message (str): 原始消息字符串
            
        Returns:
            tuple: (topic, content) 主题和内容
        """
        # 查找第一个分隔符，分割主题和内容
        first_separator_index = message.find(self.separator)
        if first_separator_index == -1:
            # 如果没有找到分隔符，整个消息作为内容，主题为空
            raise ValueError("Invalid message format")
        topic = message[:first_separator_index]
        content = message[first_separator_index + 1:]
        return topic, content

    def _handle_message(self, topic, content):
        """
        处理消息，根据主题调用相应的处理函数
        
        Args:
            topic (str): 消息主题
            content (str): 消息内容
        """
        try:
            # 尝试解析JSON内容
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                data = content
            # 查找对应主题的处理函数
            if topic in self.message_handlers:
                # 调用特定主题的处理函数
                self.message_handlers[topic](topic, data)
            elif "*" in self.message_handlers:
                # 如果注册了通配符处理函数，则调用它
                self.message_handlers["*"](topic, data)
            else:
                # 默认处理方式：记录日志
                logger.info(f"收到主题 '{topic}' 的消息: {data}")
        except Exception as e:
            logger.error(f"处理消息时出错: {e}")

    def start_listening(self):
        """开始监听消息"""
        if not self.subscriber:
            logger.error("请先连接到服务端")
            return

        if not self.authenticated:
            logger.error("未通过认证，无法接收消息")
            return

        # 如果没有订阅任何主题，默认订阅所有主题
        if not self.subscribed_topics:
            self.subscribe_topic("")  # 订阅所有主题
            logger.info("未指定订阅主题，将订阅所有主题")

        self.running = True
        logger.info("开始监听消息...")

        try:
            while self.running:
                try:
                    # 接收消息
                    message = self.subscriber.recv_string(zmq.NOBLOCK)
                    # 解析消息
                    topic, content = self._parse_message(message)
                    # 处理消息
                    self._handle_message(topic, content)
                except zmq.Again:
                    # 没有消息可接收，短暂休眠
                    time.sleep(0.1)
                except Exception as e:
                    if self.running:  # 只在运行状态下记录错误
                        logger.error(f"接收消息时出错: {e}")
                    break

        except KeyboardInterrupt:
            logger.info("收到中断信号，停止监听消息")
        finally:
            self.stop()

    def subscribe_topic(self, topic):
        """订阅特定主题"""
        if self.subscriber:
            # 使用字节数据而不是字符串进行订阅
            if isinstance(topic, str):
                self.subscriber.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
            else:
                self.subscriber.setsockopt(zmq.SUBSCRIBE, topic)
            self.subscribed_topics.add(topic)
            logger.info(f"已订阅主题: {topic}")

    def unsubscribe_topic(self, topic):
        """取消订阅特定主题"""
        if self.subscriber:
            # 使用字节数据而不是字符串进行取消订阅
            if isinstance(topic, str):
                self.subscriber.setsockopt(zmq.UNSUBSCRIBE, topic.encode('utf-8'))
            else:
                self.subscriber.setsockopt(zmq.UNSUBSCRIBE, topic)
            self.subscribed_topics.discard(topic)
            logger.info(f"已取消订阅主题: {topic}")

    def stop(self):
        """停止客户端"""
        self.running = False
        if self.subscriber:
            self.subscriber.close()
        self.context.term()
        logger.info("ZeroMQ客户端已停止")


def default_message_handler(topic, data):
    """默认消息处理函数"""
    logger.info(f"处理 {topic} 消息: {data}")


def ticker_message_handler(topic, data):
    """处理ticker主题消息的函数"""
    logger.info(f"收到{topic}数据 - {data}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="ZeroMQ客户端")
    parser.add_argument("--server", default="localhost", help="服务端地址 (默认: localhost)")
    parser.add_argument("--port", type=int, default=5555, help="订阅端口 (默认: 5555)")
    parser.add_argument("--username", default="admin", help="用户名 (默认: user12)")
    parser.add_argument("--password", default="password", help="密码 (默认: pass456)")
    parser.add_argument("--topic", default="ticker", help="订阅特定主题")
    args = parser.parse_args()
    # 确保日志目录存在
    if not os.path.exists("logs"):
        os.makedirs("logs")
    # 设置日志
    setup_logging()
    # 创建并启动客户端
    client = ZMQClient(
        server_address=args.server,
        port=args.port,
        username=args.username,
        password=args.password
    )

    # 注册消息处理函数
    client.register_message_handler("ticker", ticker_message_handler)
    client.register_message_handler("*", default_message_handler)  # 通配符处理函数

    # 连接到服务端
    if not client.connect():
        logger.error("无法连接到服务端，程序退出")
        return

    # 如果指定了主题，则订阅该主题，否则默认订阅所有主题
    if args.topic:
        client.subscribe_topic(args.topic)
        logger.info(f"已订阅指定主题: {args.topic}")
    else:
        logger.info("未指定特定主题，将订阅所有主题")

    try:
        client.start_listening()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭客户端...")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
