"""
同步数据到oss buckup 目录
"""
import os
import sys
import hashlib
import time

from loguru import logger


def get_file_md5(file_path):
    """
    获取文件md5
    :param file_path:
    :return:
    """
    # with open(file_path, 'rb') as f:
    #     md5 = hashlib.md5(f.read()).hexdigest()
    # return md5
    return ""


def calculate_file_md5(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as file:
        while True:
            data = file.read(4096)  # 每次读取4KB数据
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()


def get_files_recursive(directory) -> dict:
    """
    获取目录下所有文件
    :param directory:
    :return:
    """
    logger.info(f"处理目录：{directory}")
    rs = {}
    for root, dirs, files in os.walk(directory):

        for file in files:
            k = os.path.join(root, file)
            v = calculate_file_md5(k)
            rs[k] = v
        for dir in dirs:
            fs = get_files_recursive(os.path.join(root, dir))
            for k, v in fs.items():
                rs[k] = v
    return rs


def sync_data_to_oss():
    pass


"""
同步数据到oss buckup 目录

"""
if __name__ == '__main__':
    sync_dir = {}
    start = time.time()
    data = get_files_recursive("D:\\文档\\backup\\workspace\\datavita-andes-dpms-api")
    logger.info(f"耗时：{time.time() - start}")
    print(len(data))
    for k, v in data.items():
        print(k, v)
