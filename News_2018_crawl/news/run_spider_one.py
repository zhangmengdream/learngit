# -*- coding:utf-8 -*-
from scrapy import cmdline
from redis import *
import sys
import os
import time
import datetime


def run():


    sr.lpush('cfi_industry:start_urls',
             "http://industry.cfi.cn/BCA0A4127A4128A5063.html",
             )
    os.system("scrapy crawl cfi_industry")

if __name__ == '__main__':

    # reids_push_url
    # 创建StrictRedis对象，与redis服务器建立连接
    sr = StrictRedis(password='hyd1001@')

    while True:
        print('开始时间 %s' % datetime.datetime.now())
        with open('spiders_one_time.log', 'a') as f:
            dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write('开始时间 %s \n' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        run()
        time.sleep(600)
