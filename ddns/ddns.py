#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2021/12/03 14:00
# @Author  : haifeng
# @File    : ddns.py
# 更新ip到阿里云dns

import json
import logging
import re
import sys
import time

import requests
from aliyunsdkalidns.request.v20150109.AddDomainRecordRequest import AddDomainRecordRequest
from aliyunsdkalidns.request.v20150109.DescribeDomainRecordsRequest import DescribeDomainRecordsRequest
from aliyunsdkalidns.request.v20150109.UpdateDomainRecordRequest import UpdateDomainRecordRequest
from aliyunsdkcore.auth.credentials import AccessKeyCredential
from aliyunsdkcore.client import AcsClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s')


def param_tool(arg):
    param_dict = {}
    for i in range(0, int(len(arg) / 2)):
        k = str(arg[2 * i]).replace('-', '')
        param_dict[k] = arg[2 * i + 1]
    if param_dict.get('interval') is None:
        param_dict['interval'] = 60
    for k, v in param_dict.items():
        logging.info('参数: {} -> {}'.format(k, v))
    return param_dict


def get_record(domain, rr, credentials):
    client = AcsClient(region_id='cn-beijing', credential=credentials)
    request = DescribeDomainRecordsRequest()
    request.set_accept_format('json')
    request.set_DomainName(domain)
    response = client.do_action_with_exception(request)
    json_resp = json.loads(str(response, encoding='utf-8'))
    records_ = json_resp['DomainRecords']['Record']
    for record in records_:
        rr_ = record['RR']
        if str(rr).lower() == str(rr_).lower():
            logging.info('历史记录：{}'.format(str(record)))
            return dict(record)
    return None


def add_record(doamin, rr, ip, credentials):
    logging.info('添加记录{}.{} -> {}'.format(rr, doamin, ip))
    client = AcsClient(region_id='cn-beijing', credential=credentials)
    request = AddDomainRecordRequest()
    request.set_accept_format('json')
    request.set_DomainName(doamin)
    request.set_RR(rr)
    request.set_Type("A")
    request.set_Value(ip)
    response = client.do_action_with_exception(request)
    logging.info(str(response, encoding='utf-8'))


def update_record(record_id, rr, ip, credentials):
    client = AcsClient(region_id='cn-beijing', credential=credentials)
    request = UpdateDomainRecordRequest()
    request.set_accept_format('json')
    request.set_RecordId(record_id)
    request.set_RR(rr)
    request.set_Type("A")
    request.set_Value(ip)
    response = client.do_action_with_exception(request)
    logging.info(str(response, encoding='utf-8'))


def get_ip():
    try:
        response = requests.get('https://jsonip.com/')
        resp = json.loads(response.text)
        logging.info('https://jsonip.com/ 获取ip-->{}'.format(resp['ip']))
        return resp['ip']
    except Exception as e:
        logging.error(e)
        logging.warning('jsonip获取ip失败，尝试使用https://ip.tool.lu/')
        try:
            response = requests.get('https://ip.tool.lu/')
            ip = re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", response.text)
            logging.info('https://ip.tool.lu/ 获取ip-->{}'.format(ip))
            return ip[0]
        except Exception as e:
            logging.error(e)
            logging.error('https://ip.tool.lu/ 获取ip失败，程序退出')
            sys.exit(1)


if __name__ == '__main__':
    argv = sys.argv
    if (len(argv) - 1) % 2 != 0:
        logging.error('参数必须成对')
    param_dict = param_tool(argv[1:])
    credentials = AccessKeyCredential(param_dict['access_key_id'], param_dict['access_key_secret'])
    interval = int(param_dict['interval'])
    domain = param_dict['domain']
    rr = param_dict['RR']
    while True:
        record = get_record(domain, rr, credentials)
        ip = get_ip()
        if record is None:
            add_record(domain, rr, ip, credentials)
        else:
            if record['Value'] == ip:
                logging.info('{} 与记录ip一致无需更新')
            else:
                update_record(record['RecordId'], rr, ip, credentials)
                logging.info('更新ip记录: {}->{}'.format(record['Value'], ip))
        time.sleep(interval)
