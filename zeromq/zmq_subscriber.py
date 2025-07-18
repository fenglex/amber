# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/18 14:51
# @Version: 1.0
import zmq

def subscriber(topic_filter="NEWS"):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)  # 创建订阅套接字
    socket.connect("tcp://localhost:5556")  # 连接到发布者

    # 订阅指定主题（默认订阅所有）
    socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)

    print(f"订阅者已启动，过滤主题: {topic_filter}")
    while True:
        # 接收消息（主题和内容自动分离）
        topic, message = socket.recv_string().split(" ", 1)
        print(f"[{topic}] 收到消息: {message}")


if __name__ == '__main__':
    subscriber()
