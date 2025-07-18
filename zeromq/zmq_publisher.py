# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: haifeng
# @Date: 2025/7/18 14:50
# @Version: 1.0

import zmq
from zmq.auth.thread import ThreadAuthenticator
import zmq.auth
import os

# 预设合法用户凭据
VALID_CREDENTIALS = {
    "user1": "pass123",
    "admin": "secret456"
}


def zap_handler(context, socket):
    """ZAP 协议认证处理器"""
    while True:
        # 接收 ZAP 认证请求（格式：[0](@ref)username:password）
        msg = socket.recv_multipart()
        if not msg:
            break
        # 解析 ZAP 消息结构（参考 RFC 27）
        version, sequence, domain, mechanism, credentials = msg
        username, password = credentials.decode().split(":")

        # 验证用户名和密码
        if VALID_CREDENTIALS.get(username) == password:
            # 认证成功，返回 ZAP 响应码 0
            socket.send_multipart([version, b"0", b"OK"])
        else:
            # 认证失败，返回 ZAP 响应码 1
            socket.send_multipart([version, b"1", b"FAIL"])


def secure_server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    # 启动 ZAP 认证线程
    auth = ThreadAuthenticator(context)
    auth.start()

    # 配置 ZAP 认证处理器
    auth.curve_serverkey = b"server_public_key"  # 服务端公钥（需提前生成）
    auth.curve_publickey = b"client_public_key"  # 客户端公钥
    auth.curve_secretkey = b"server_secret_key"  # 服务端私钥

    # 注册 ZAP 处理器
    auth.set_zap_handler(zap_handler)  # 修正：通过 auth 实例注册

    # 启用 Curve 加密
    socket.curve_publickey = auth.curve_publickey
    socket.curve_secretkey = auth.curve_secretkey
    socket.curve_serverkey = auth.curve_serverkey

    socket.bind("tcp://*:5555")
    print("Secure Server with ZAP is running...")

    while True:
        message = socket.recv_string()
        print(f"Received: {message}")
        socket.send_string(f"Echo: {message}")

if __name__ == '__main__':
    secure_server()
