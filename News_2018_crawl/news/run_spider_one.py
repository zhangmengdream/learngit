# -*- coding:utf-8 -*-
from scrapy import cmdline
from redis import *
import sys
import os
import time
import datetime


def run():

    sr.lpush('abroad_seeking_alpha_efts:start_urls',
             "https://seekingalpha.com/etfs-and-funds/etf-analysis?page=1"
             )
    os.system("scrapy crawl abroad_seeking_alpha_efts")

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
