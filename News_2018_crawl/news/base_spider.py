# -*- coding: utf-8 -*-
import hashlib
import re
import time
import datetime
import requests
from scrapy_redis.spiders import RedisSpider
from scrapy.exceptions import CloseSpider, DontCloseSpider
from redis import *
from news.settings import MAX_IDEL_NUM, Redis_settings, Mongo_settings, proxies
import json


class Base_Spider(RedisSpider):
    custom_settings = {
        # 只scrapy-redis插件去重
        # 'DUPEFILTER_CLASS': "scrapy_redis.dupefilter.RFPDupeFilter",
        # 'SCHEDULER': "scrapy_redis.scheduler.Scheduler",
        # 'SCHEDULER_PERSIST': True,
        'DOWNLOADER_MIDDLEWARES': {
            'news.middlewares.MyUserAgentMiddleware': 380,
            'news.middlewares.ProxiesMiddleware': 390,
        },
        # 重试次数
        "DOWNLOAD_TIMEOUT": 20,
        "DOWNLOAD_DELAY": 2,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 20,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 429, 302],
        'HTTPERROR_ALLOWED_CODES': []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
    }

    def __init__(self, name, redis_key, onseturl=None, *args, **kwargs):
        super().__init__()
        self.name = name
        self.redis_key = redis_key
        # 获取start_urls的数量 来计算重复多少次结束爬虫的数量

        self.sr = StrictRedis(host=Redis_settings['host'], port=Redis_settings['port'],
                              )

        self.redis_cli = StrictRedis(db=1, host=Redis_settings['host'], port=Redis_settings['port'],
                                     decode_responses=True)

        self.start_url = self.sr.lrange(self.name + ':start_urls', 0, -1)
        # 获取爬虫对应的分类 sector
        if onseturl:
            print('onseturl:', onseturl)
            self.onseturl = onseturl
            m = hashlib.md5()
            m.update(bytes(self.onseturl, encoding='utf-8'))
            sector = m.hexdigest()
            self.sector = self.sr.hget('watch_list', sector).decode('utf-8')
        else:
            self.sector = '综合'
            # # 功能1-1 继承，并开启mongo数据库连接 (create_in: 18-09-05)
        from pymongo import MongoClient
        # self.mongodb_client = MongoClient(Mongo_settings['host'], Mongo_settings['port'])
        self.mongodb_client = MongoClient(Mongo_settings['host_port'])

        if Mongo_settings.get('user'):
            self.mongodb_client.get_database('admin').authenticate(name=Mongo_settings['user'],
                                                                   password=Mongo_settings['pwd'],
                                                                   mechanism=Mongo_settings['mechanism'])
        self.collection = self.mongodb_client.get_database('News').get_collection(self.name)
        self.has_finished = False
        self.repeat_count = 0
        self.num_rest = 0
        self.REPEAT_MAX_COUNT = 30
        self.pattern_sub = re.compile(r'\u3000|\s|\xa0|\ufeff|\u200b|\ue5e4|\ue5e5')

        self.col = self.mongodb_client.get_database('Repeat').get_collection(self.name + '_repeat_url')
        # self.col = self.db.get_collection(self.name + '_repeat_url')
        self.repeat_info = self.col.find_one({'_id': self.start_url[0].decode('utf-8')})
        # 如果不是第一次运行此爬虫则使用这个去重的方法，否则不使用去重
        if self.repeat_info:
            self.now_repeat_url = self.repeat_info['repeat_url']
        else:
            # 如果获取不到说明是第一次运行  赋初始值为None
            self.now_repeat_url = None
        # 初始化更新时的去重链接和id
        self.update_repeat_url = None
        self._id = None

    def nDayAgo(self, n):
        return (datetime.datetime.now() - datetime.timedelta(days=n)).strftime('%Y-%m-%d %H:%M:%S')

    # 功能1-2 对详情页url进行集合内查询，查到就基本说明往后都是旧文章了，随机停止爬虫，不用做无用翻页了 (create_in: 18-09-05)
    def repeat_num(self, url):

        self.repeat_nums = self.collection.find({'news_url': {'$regex': url.replace('?', '.')}}).count()
        if self.repeat_nums > 0:
            self.repeat_count += 1
            self.redis_cli.set(self.name + '_repeat_count', self.repeat_count)
            print("repeat_count------------------------------:", self.repeat_count)

        if self.repeat_count >= self.REPEAT_MAX_COUNT:
            print(self.name + '爬完包含新闻的页面,停止翻页')
            self.has_finished = True
            print("爬完包含新闻的页面,停止翻页 , spider is closed!")
            raise CloseSpider
        if self.repeat_nums > 0:
            return True

    def get_index(self, url):
        self.collection.create_index({'news_url': 1}, {'unique': 'true'})

    # 去除列表中的空格
    # def rm_list_filde(self, list):
    #     while '' in list:
    #         list.remove('')
    #     return list

    def not_empty(self, s):
        return s and s.strip()

    def rm_list_filde(self, _list):
        return list(filter(self.not_empty, _list))

    # 将日期格式规整为两位
    def news_date_info(self, date):
        if len(date) == 1:
            date = '0' + str(date)
        return date

    # 先转化成时间戳，然后再格式化
    def news_date_all(self, publish_time):
        timeArray = time.strptime(publish_time, "%Y-%m-%d %H:%M:%S")
        timent = time.localtime(int(time.mktime(timeArray)))
        publish_time = time.strftime('%Y-%m-%d %H:%M:%S', timent)
        return publish_time

    def stamp_to_time(self, publish_time):
        timent = time.localtime(int(publish_time))
        publish_time = time.strftime('%Y-%m-%d %H:%M:%S', timent)
        return publish_time

    # 判断是否为json
    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except Exception:
            return {}
        return json_object

    # 判断 如果有链接 并且是第一页的情况下  记录这个链接 和 列表链接  (翻页为下一页的情况下)
    def save_repeat_url(self, response, url, page):
        if page:
            page_num = int(page)
        else:
            page_num = response.xpath(
                '//a[contains(.,"下一页")]/@href|//a[@class="next"]/@href|//a[contains(.,">")]/@href').extract()
            page_num = ''.join(page_num)
            page_num = re.match('.*?(\d+)\.[html|shtml|htm]', page_num)
            page_num = int(page_num.group(1))

        # 下一页是第二页 说明当前页面是第一页  将第一页的第一个链接存储进mongo为下次的去重链接  下一页页码是1 或者2  因为有的页面是从0 页开始的
        if page_num in [1, 2] and url:
            #  存储redis做缓存
            self._id = response.url
            self.update_repeat_url = url
            # 存储去重redis
            # redis_cli.set('repeat_url' + self.name, {'_id': self._id, 'repeat_url': self.update_repeat_url})
            # 如果有缓存的去重链接 则更新去重链接
            if self.update_repeat_url:
                if self.col.find({'_id': self._id}).count() != 0:
                    self.col.update({'_id': self._id}, {'$set': {"repeat_url": self.update_repeat_url}})
                    print('-------------更新存储记录链接----------------')
                else:
                    repeat_info = {'_id': self._id, 'repeat_url': self.update_repeat_url}
                    self.col.insert_one(dict(repeat_info))
                    print('-------------第一次存储记录链接----------------')

    # 接受队列闲置信号
    def spider_idle(self):
        self.num_rest += 1
        print('self.num_rest ' + str(self.num_rest))
        if self.num_rest < MAX_IDEL_NUM:
            self.schedule_next_requests()
            raise DontCloseSpider
        else:
            # # 如果有缓存的去重链接 则更新去重链接
            # if self.update_repeat_url:
            #     if self.col.find({'_id': self._id}).count() != 0:
            #         self.col.update({'_id': self._id}, {'$set': {"repeat_url": self.update_repeat_url}})
            #         print('-------------更新存储记录链接----------------')
            #     else:
            #         repeat_info = {'_id': self._id, 'repeat_url': self.update_repeat_url}
            #         self.col.insert_one(dict(repeat_info))
            #         print('-------------第一次存储记录链接----------------')

            print("spider is closed!")
            raise CloseSpider

    # 先转化成时间戳，然后再格式化
    # timeArray = time.strptime(publish_time, "%Y-%m-%d %H:%M:%S")
    # timent = time.localtime(int(time.mktime(timeArray)))
    # publish_time = time.strftime('%Y-%m-%d %H:%M:%S', timent)

    def proxies_get_url_except(self, url, headers=headers, status=[200], timeout=30):
        print(status)
        while True:
            try:
                res = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
                print(res.status_code)
                if res.status_code in status:
                    return res
            except:
                print('重新get_url：{}'.format(url))
