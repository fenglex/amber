"""
同步数据到oss buckup 目录
"""
import os
import sys
import hashlib


def get_file_md5(file_path):
    """
    获取文件md5
    :param file_path:
    :return:
    """
    with open(file_path, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    return md5


def get_files_recursive(directory):
    """
    获取目录下所有文件
    :param directory:
    :return:
    """
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files


def sync_data_to_oss():
    pass


"""
同步数据到oss buckup 目录
"""
if __name__ == '__main__':
    sync_dir = {}
