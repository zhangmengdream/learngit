# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import hashlib
import json
import re
import os
from urllib import parse
from pymongo import *
from tools.spider_type import get_spider_type
# from tools.follow_noticer import Noticer
from .settings import Mongo_settings, WEB_MAP, ES, Redis_settings
import logging
import datetime
from elasticsearch import Elasticsearch
import time
from scrapy.exceptions import CloseSpider
from .notices import to_wx_error_info, ERROR
from redis import *
from .wechat_alert import WechatNotice
import traceback
import jieba
from collections import Counter
from lxml import etree
from jieba.analyse import extract_tags
from simhash import Simhash
from news.settings import Repeat_db, redis_cli, sr
from redis import *

# 微信发送者客户端
wx_noticer = WechatNotice()
# 添加公司名称词库
jieba.load_userdict("company.txt")
loger = logging.getLogger(__name__)


class NewsPipeline(object):
    '''
    通用pipeline
    '''

    def open_spider(self, spider):
        if 'xueqiu' in spider.name:
            spider.name = 'xueqiu_people'

        try:
            self.start_url = sr.lrange(spider.name + ':start_urls', 0, -1)[-1]
        except:
            self.start_url = ""

        # 开启redis_cli
        try:
            self.redis_cli = redis_cli
            # 先查询该爬虫信息key是否存在
            if_exist = self.redis_cli.exists(spider.name)
            if if_exist is False:
                # 如果没有， 那么加上该错误信息
                # 错误信息
                error_info = {
                    'error_times': 0,
                    'error_info': '',
                    # 每次报错只报第一个错误， 并且爬虫关闭后将first_error重置为0
                    'fisrt_error': 0,
                }
                self.redis_cli.hmset(spider.name, error_info)
            else:
                error_info = {
                    'error_times': 0,
                    'error_info': '',
                    # 每次报错只报第一个错误， 并且爬虫关闭后将first_error重置为0
                    'fisrt_error': 0,
                }
                self.redis_cli.hmset(spider.name, error_info)
        except Exception:
            wx_noticer.send_msg('Redis 连接失败\n' + traceback.format_exc())
        # 记录该爬虫的错误次数， 爬虫结束时， 如果再次统计次数跟开始时的差为0， 那就没有错误， 可以重置为0， 并清空错误信息
        self.error_times_start = eval(self.redis_cli.hget(spider.name, 'error_times'))

        # 开启elasticsearch client
        try:
            self.es = Elasticsearch(
                hosts=[{'host': ES['host'], 'port': ES['port']}],
                http_auth=(ES['auth'], ES['password'])
            )
        except Exception as e:
            # 发送错误报告给微信
            to_wx_error_info(app_cate='新闻爬虫', app_name=spider.name, error_level=2,
                             error_short='无法连接 elastic')
            raise CloseSpider

        # 开启mongodb client
        try:
            self.client = MongoClient(Mongo_settings['host_port'])
            if Mongo_settings.get('user'):
                self.client.get_database('admin').authenticate(name=Mongo_settings['user'],
                                                               password=Mongo_settings['pwd'],
                                                               mechanism=Mongo_settings['mechanism'])
            self.db = self.client.get_database('News')
            self.col = self.db.get_collection(spider.name)
            self.repeat_col = Repeat_db.get_collection(spider.name)
            # 倒排索引  数据库
            self.information = self.client.get_database('information')
            self.notice = self.client.get_database('notice')
            self.research = self.client.get_database('research')
            self.wemedia = self.client.get_database('wemedia')
            # 爬虫板块 和 网站 关联集合， 用于查询新闻在整个网站得所有板块集合中是否存在
            self.site_cols_map_col = self.db.get_collection('site_cols_map')


        except Exception as e:
            # 发送错误报告给微信
            to_wx_error_info(app_cate='新闻爬虫', app_name=spider.name, error_level=2,
                             error_short='无法连接 mongodb')
            raise CloseSpider

        self.start_num = self.col.count()  # 爬取前数量
        loger.info('爬取' + WEB_MAP.get(spider.name, spider.name) + '前数量为%s' % self.start_num)
        self.start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.redis_cli.delete(spider.name + '_start_num_count')
        self.redis_cli.set(spider.name + '_start_num_count', self.start_num)

        # 本爬虫的相关爬虫mongo集合名列表，用于存入数据库前去重（因为不同板块爬虫爬的同一篇文章， 只会比较存入自己的集合，因此会在mongo存入同一网站相同文章多次， 导致多次存入ES）
        self.cols_list = []
        self.exclude_spider = ['people_bank',
                               'xueqiu_top',
                               'huatai_strategy',
                               'huatai_hangqing',
                               ]  # 这些排除录入 mongo 网站对应集合的 collection
        self.spider = spider
        # self.noticer = Noticer()

    def close_spider(self, spider):

        # 先关闭爬虫开启的链接
        # spider.mongodb_client.close()
        # 检查redis信息， 看是否报错给手机微信
        # 爬虫结束时， 如果再次统计次数跟开始时的差为0， 那就没有错误， 可以重置为0， 并清空错误信息
        self.error_times_end = eval(self.redis_cli.hget(spider.name, 'error_times'))
        if self.error_times_end - self.error_times_start != 0:
            msg = self.redis_cli.hget(spider.name, 'error_info')
            wx_noticer.send_msg(msg.decode())
        else:
            # 如果是0 那么应该清空redis里 本爬虫 的错误信息，和次数
            self.redis_cli.hmset(spider.name, {'error_times': 0})
            self.redis_cli.hmset(spider.name, {'error_info': ''})

        # 重置一下第一次错误信息为0
        self.redis_cli.hmset(spider.name, {'fisrt_error': 0})
        # 关闭redis链接
        # self.redis_cli.connection_pool.disconnect()

        # try:
        #
        # 记录一下爬取的数量
        self.end_num = self.col.count()
        loger.info('爬取' + WEB_MAP.get(spider.name, spider.name) + '后数量为%s' % self.end_num)
        loger.info('新增条数为 %s\n' % (self.end_num - self.start_num))
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 抓取的爬虫条数
        spider_count = self.end_num - self.start_num

        # 从redis中获取爬取的状态 存储 mongo
        spider_now_func = self.redis_cli.get(spider.name + '_now_func')
        spider_repeat_count = self.redis_cli.get(spider.name + '_repeat_count')

        # 删除此次记录的最后的函数名和计数  以免下次使用混淆
        self.redis_cli.delete(spider.name + '_now_func')
        self.redis_cli.delete(spider.name + '_repeat_count')

        if spider_now_func:
            if spider_now_func == 'parse' or spider_now_func == 'start_requests':
                if spider_repeat_count:
                    spider_state = '无新更新文章'
                else:
                    spider_state = '列表解析出现了问题'
            elif spider_now_func == 'parse_detail':
                if spider_count:
                    spider_state = '正常状态'
                else:
                    if spider_repeat_count:
                        spider_state = '无新更新文章'
                    else:
                        spider_state = '文章细节解析出现了问题'
            else:
                spider_state = '正常状态'
        else:
            spider_state = '未进入爬取状态,爬虫链接失效'
        print('--------------------------------------------------3')

        # 在数据库中进行记录
        '''
        last_content: 最后获取内容的时间
        last_time:    最后抓取的时间
        last_err_time:最后报错的时间
        '''

        m = hashlib.md5()
        m.update(bytes(WEB_MAP.get(spider.name, spider.name) + spider.name, encoding='utf-8'))
        _id = m.hexdigest()
        last_info = list(self.col.find({"_id": _id}, {'last_content': 1, 'last_err_time': 1, "crawl_date": 1}))
        if last_info:
            last_content = last_info[0]['last_content']
            last_err_time = last_info[0]['last_err_time']
        else:
            last_content = dt
            last_err_time = '无'

        if spider_count:
            last_content = dt
        else:
            last_content = last_content

        #   新加的 第一次抓取时间
        if last_info:
            if last_info[0].get('crawl_date'):
                crawl_date = last_info[0].get('crawl_date')
            else:
                datas = self.col.find({}, {"crawl_date": 1}).sort([("crawl_date", -1)]).limit(5)
                crawl_dates = [i['crawl_date'] for i in list(datas) if i.get("crawl_date")]
                crawl_date = crawl_dates[0]
        else:
            datas = self.col.find({}, {"crawl_date": 1}).sort([("crawl_date", -1)]).limit(5)
            crawl_dates = [i['crawl_date'] for i in list(datas) if i.get("crawl_date")]
            crawl_date = crawl_dates[0]

        last_grasp_time = {
            'last_content': last_content,
            'last_time': dt,
            'last_err_time': last_err_time,
            'spider_state': spider_state,
            'create_date': crawl_date
        }
        log_info = last_grasp_time
        # 计算用时
        startTime2 = datetime.datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
        endTime2 = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        seconds = float("%.2f" % ((endTime2 - startTime2).seconds / 60))
        # rotation_count = sr.get(spider.name + '_rotation_count')
        rotation_count = 1

        # print(self.block_type)
        # print(self.es_index)

        log_info.update(
            {"col_name": spider.name, "web_block_name": WEB_MAP.get(spider.name, spider.name), "use_time": seconds,
             "start_time": self.start_time, "start_url": self.start_url, "add_num": spider_count,
             "round_count": int(rotation_count),
             "crawl_date": crawl_date})
        log_info = re.sub("'", '"', str(log_info))
        print('log_info:----------', log_info)
        # with open('/home/mikefiles/scrapy_project/News_2018/news/spider_log/spider_' + spider.name + '.log', 'w', encoding='utf-8') as f:
        #     f.write(log_info + '\n')

        # 如果已经存在字段  更新
        if last_info:
            self.col.update({"_id": _id}, last_grasp_time)  # 更新
        else:
            last_grasp_time.update({"_id": _id})
            self.col.insert_one(last_grasp_time)
            self.col.ensure_index('news_url', unique=False)

            # 关闭数据库
            # self.client.close()

        # except Exception as e:
        #     print(e)

    def filter_author(self, author_info):
        author_info = re.sub('\u200b|\n', '', author_info)

        if author_info == '无':
            return []
        if "：" in author_info:
            author_list = [re.match('.*?：(.*?)$', author_info).group(1)]
        elif author_info.strip() == 'I/O':
            author_list = ['I/O']
        else:
            author_info = re.sub(',|，|、|/', ',', author_info)
            author_info = author_info.replace('\\', ',').replace('|', ',')
            if ',' in author_info:
                author_list = author_info.split(',')
            else:
                # 如果包含中文  再做一次空格切割（否则就是英文名称，只做分隔符切割）
                if self.is_contain_chinese(author_info):
                    author_list = author_info.strip().split(' ')
                else:
                    author_list = [author_info]

        author_list = self.not_empty(author_list)
        return author_list

    # 判断字符串中是否包含中文
    def is_contain_chinese(self, check_str):
        for ch in check_str:
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
        return False

    # 过滤掉列表的空白元素   去除每个作者前后两边空格
    def not_empty(self, _list):
        return list(map(lambda x: x.strip(), list(filter(lambda y: y.strip(), _list))))

    def filter_futures_keyword(self, item):
        if 'plate_unique' in item and item['plate_unique'] == '期货':
            filter_words = ['苹果公司', '苹果股价', '特斯拉']
            if False in list(map(lambda key: key not in item['content'], filter_words)):
                print('过滤掉，不要了')
                with open('期货根据关键词过滤掉的文章.txt', 'a') as f:
                    f.write(item['news_url'] + '\n')
                return True
            else:
                return False
        else:
            return False

    def process_item(self, item, spider):
        item = self.wash_data(item)
        filter_futures_keyword = self.filter_futures_keyword(item)
        if filter_futures_keyword:
            return

        if 'doc_type' in item and item['doc_type'] == 'notice':
            type = 'notice'
        else:
            type = None
        item['author'] = re.sub('<.*?>', '', item['author'])
        item['author_list'] = self.filter_author(item['author'])
        try:
            is_saved = self.save_to_mongodb(item, spider, type)
            item = is_saved[1]

        except Exception as e:
            # 发送错误报告给微信
            to_wx_error_info(app_cate='新闻爬虫', app_name=spider.name, error_level=2,
                             error_short='mongodb插入数据失败')
            raise CloseSpider

        if is_saved[0]:  # 插入mongo成功，再去插入es引擎
            try:

                self.save_to_elasticsearch(item, spider, is_saved[-1])
                print('存入es')
                print(item['news_url'])
            except Exception as e:
                print(e)
                # 发送错误报告给微信
                to_wx_error_info(app_cate='新闻爬虫', app_name=spider.name, error_level=2,
                                 error_short='elastic插入数据失败\n' + item['news_url'])
                raise CloseSpider
        else:
            loger.info('数据存在不存入es')

        return item

    def wash_data(self, item):
        # 这块暂时不用
        # item['abstract'] = re.sub(r'<.*?>|\xa0|\u3000|\t|\n|\s|\ufeff|\u200b|\uf06c|\x7f', '', item['abstract'])
        loger.info('抓取信息:\n' + str(item))
        return item

    def extractTag(self, str):
        return extract_tags(str, topK=20, withWeight=True)

    # 文章不符合存储条件 （此处判断如果是一个月之前的文章，不存储----- 排除部分特殊网站）
    def news_off_grade(self, item, spider):
        if 'xueqiu' not in spider.name and item.get('plate_unique') != '私募' and spider.name not in [
            "aigupiao_author_id", 'futures_96369']:
            a_month_ago = (datetime.datetime.now() + datetime.timedelta(days=-200)).strftime('%Y-%m-%d %H:%M:%S')
            # 符合以下条件，停止
            if a_month_ago > item['news_date']:
                spider.crawler.engine.close_spider(spider, '%s 之后的文章已经抓取，停止爬虫' % item['news_date'])
                return True
            else:
                return False
        else:
            return False

    def save_to_mongodb(self, item, spider, type=None):

        item['title'] = re.sub('>','',item['title'])
        # if self.news_off_grade(item, spider):
        #     return False, item

        sp_name = spider.name
        # 录入数据库之前 对 爬虫相关的集合都进行查询，确保不会重复录入同一篇到不同集合
        if sp_name not in self.exclude_spider:
            site_filed = sp_name.split('_')[0]  # 网站名
            num = self.site_cols_map_col.find({'site': site_filed}).count()
            # 在映射集合查询有没有此网站信息，没有得话就把该网站 和 此爬虫（集合）加进去。。。
            if num == 0:
                doc = {'site': site_filed, 'cols': [sp_name]}
                self.site_cols_map_col.insert_one(doc)
            cols = self.site_cols_map_col.find({'site': site_filed})[0].get('cols')

            # 判断这个爬虫名字(集合)在不在 对应网站得集合信息列表
            if sp_name not in cols:
                # 不在集合里面 要加进去
                cols.append(sp_name)
                self.site_cols_map_col.update_one({'site': site_filed}, {'$set': {'cols': cols}})

            exist = False  # 文章是否存在flag
            # 查询相关得集合，看是否有此item存在
            for col in cols:
                if self.db.get_collection(col).find({'_id': item['_id']}).count() != 0:
                    exist = True
                    break

            # 如果查询本网站相关集合后没有发现同一篇文章， 那么就录入当前爬虫集合吧
            if exist == False:
                try:
                    print('debug1----------------------------------------------------------1-1')
                    html_str, pure_txt = self.clear_content_with_tags(item['title'], item['content'], item['news_url'],
                                                                      spider, type)
                    # 统计公司数量前， 用纯文本
                    item, company_list = self.statistics_company_count(item)
                    pure_txt = self.delete_content_brackets(company_list, pure_txt)
                    item['simhash_val'] = str(Simhash(self.extractTag(pure_txt)).value)
                    # 时间格式化
                    timeArray = time.strptime(item['news_date'], "%Y-%m-%d %H:%M:%S")
                    timent = time.localtime(int(time.mktime(timeArray)))
                    item['news_date'] = time.strftime('%Y-%m-%d %H:%M:%S', timent)
                    # 如果文章是一月之前的文章  不进行存储
                    # if self.news_off_grade(item, spider):
                    #     return False, item
                    # 之前摘要可能有标签， 有标签的 替换为 取文章纯文本的前一百个字符
                    if len(pure_txt.strip()):
                        item['abstract'] = re.sub('\s', '', pure_txt)[:160].strip() + '...'
                    else:
                        item['abstract'] = item['title']
                    # 抓取时间
                    nt = time.localtime(int(time.time()))
                    item['crawl_date'] = time.strftime('%Y-%m-%d %H:%M:%S', nt)
                    print('debug1----------------------------------------------------------1')
                    item['read_num'] = item.get('read_num') if item.get('read_num') else '无'
                    item['comment_num'] = item.get('comment_num') if item.get('comment_num') else '无'
                    item['comment_list'] = item.get('comment_list') if item.get('comment_list') else '无'
                    item['news_no'] = item.get('news_no') if item.get('news_no') else '无'
                    # item['news_no'] = '无'
                    print('debug2----------------------------------------------------------1')
                    # 加入统计公司字段后， 改为带标签的正文
                    item['content'] = html_str
                    self.col.insert_one(dict(item))
                    return True, item, pure_txt
                except Exception as e:
                    print(e, 'mongo 插入失败')
                    return False, item
            else:
                return False, item
        # 排除之外的几个爬虫还走旧逻辑
        else:
            # 为防止重复， 存入前先查询是否存在然后判断存在与否 （雪球增量功能失效）
            result = self.col.find({'_id': item['_id']}).count()
            if result == 0:
                try:
                    html_str, pure_txt = self.clear_content_with_tags(item['title'], item['content'], item['news_url'],
                                                                      spider, type)
                    company_list = self.statistics_company_count(item)
                    item, pure_txt = self.delete_content_brackets(company_list, pure_txt)
                    item['simhash_val'] = str(Simhash(self.extractTag(pure_txt)).value)
                    # 时间格式化
                    timeArray = time.strptime(item['news_date'], "%Y-%m-%d %H:%M:%S")
                    timent = time.localtime(int(time.mktime(timeArray)))
                    item['news_date'] = time.strftime('%Y-%m-%d %H:%M:%S', timent)
                    # 如果文章是一月之前的文章  不进行存储
                    if self.news_off_grade(item, spider):
                        return False, item
                    # 之前摘要可能有标签， 有标签的 替换为 取文章纯文本的前一百个字符
                    if len(pure_txt.strip()):
                        item['abstract'] = re.sub('\s', '', pure_txt)[:160].strip() + '...'
                    else:
                        item['abstract'] = item['title']
                    # 抓取时间
                    nt = time.localtime(int(time.time()))
                    item['crawl_date'] = time.strftime('%Y-%m-%d %H:%M:%S', nt)
                    print('debug1----------------------------------------------------------2')
                    item['read_num'] = item.get('read_num') if item.get('read_num') else '无'
                    item['comment_num'] = item.get('comment_num') if item.get('comment_num') else '无'
                    item['comment_list'] = item.get('comment_list') if item.get('comment_list') else '无'
                    item['news_no'] = item.get('news_no') if item.get('news_no') else '无'
                    print('debug2----------------------------------------------------------2-1')
                    # 加入统计公司字段后， 改为带标签的正文
                    item['content'] = html_str
                    self.col.insert_one(dict(item))
                    return True, item, pure_txt

                except Exception as e:
                    print(e, 'mongo 插入失败')
                    return False, item
            else:
                return False, item

    def str_to_regex_pure_txt(self, string):
        string = re.sub('\(', '（', string)
        string = re.sub('\)', '）', string)
        # string = re.sub(r'\*', '\*', string)
        # string = re.sub(r'\+', '\+', string)
        # string = re.sub(r'\-', '\-', string)
        # string = re.sub(r'\?', '\?', string)
        # string = re.sub(r'\|', '\|', string)
        # string = re.sub(r'\\', r'\\\\', string)
        string = re.sub('\[', '【', string)
        string = re.sub('\]', '】', string)
        return string

    # def save_to_elasticsearch(self, item, spider, pure_txt):
    #     # 存储es去掉括号中的内容
    #     # 存储es去掉括号中的内容
    #     pure_txt = self.str_to_regex_pure_txt(pure_txt)
    #     ch_ = set(re.findall('（.*?）', pure_txt))
    #
    #     try:
    #         for i in sorted(ch_, key=len, reverse=True):
    #             try:
    #                 if i.count('（') == 2 and i.count('）') == 1:
    #                     pure_reg = r'.*?(' + i + '.*?）).*?'
    #                     i = re.match(pure_reg, pure_txt, flags=re.S).group(1)
    #             except:
    #                 print('遇到正则问题  只匹配当前情况')
    #             pure_txt = pure_txt.replace(i, '')
    #     except:
    #         with open('error_括号.txt', 'a') as f:
    #             f.write(item['news_url'] + '\n' + str(ch_) + '\n\n')
    #
    #     # es里面正文存 纯文本
    #     doc = dict(
    #         title=item['title'],
    #         topic=item['topic'],
    #         abstract=item['abstract'],
    #         content=pure_txt,
    #         crawl_date=item['crawl_date'],
    #         news_date=item['news_date'],
    #         news_url=item['news_url'],
    #         referer_web=item['referer_web'],
    #         referer_url=item['referer_url'],
    #         author=item['author'],
    #         read_num=item['read_num'] if item['read_num'] != '无' else 0,
    #         comment_num=item['comment_num'] if item['comment_num'] != '无' else 0,
    #         keywords=item['keywords'],
    #         source=item['source'],
    #         sector=item.get('sector') if item.get('sector') is not None else '综合',
    #         tags=item['tags'],
    #         order_date=int((time.mktime(time.strptime(item['news_date'], '%Y-%m-%d %H:%M:%S')) + 14400 * 2) / 86400),
    #         # 2018-9-6 新增传入es字段
    #         catalogue=item['catalogue'],
    #         pic=item['pic'],
    #         simhash_val=item['simhash_val'],
    #
    #         # 2018-12-28 增加 公司数量统计 company_count_str    和    文章长度 content_length
    #         company_count_str=item.get('company_count'),
    #         content_length=item.get('content_length'),
    #         col_name=spider.name,
    #         thumb_num=0,  # 自己的点赞数
    #         collection_num=0,  # 自己的收藏数
    #         comments_num=0,  # 自己的评论数
    #         transmit_num=0,  # 自己的转发数
    #         author_list=item.get('author_list'),
    #
    #     )
    #     if 'author_id' in item:
    #         doc.update(
    #             author_id=item['author_id'],
    #         )
    #     if 'plate_unique' in item:
    #         doc.update(
    #             plate_unique=item['plate_unique'],
    #         )
    #     if 'company' in item:
    #         doc.update(
    #             company=item['company'],
    #         )
    #     if 'doc_type' in item:
    #         doc.update(
    #             doc_type=item['doc_type'],
    #         )
    #
    #     if item['plate_unique'] == '私募':
    #         self.es.index(index=ES['index_private'], doc_type=ES['doc_type'], body=doc, id=item['_id'])
    #
    #     elif spider.name == 'hooyuu_column':
    #         doc.update(
    #             add_hytt=1,
    #             list_col=item['list_col'],
    #             list_cp=item['list_cp'],
    #             list_label=item['list_label']
    #         )
    #         self.es.index(index=ES['index_total'], doc_type=ES['doc_type'], body=doc, id=item['_id'])
    #         # self.noticer.judge_enter_notice(item, ES['index_column'], doc_type=ES['doc_type'])
    #     else:
    #         self.es.index(index=ES['index_total'], doc_type=ES['doc_type'], body=doc, id=item['_id'])
    #         # self.noticer.judge_enter_notice(item, ES['index_news_en'], doc_type=ES['doc_type'])

    def remove_brackets(self, item, pure_txt):
        pure_txt = self.str_to_regex_pure_txt(pure_txt)
        ch_ = set(re.findall('（.*?）', pure_txt))

        try:
            for i in sorted(ch_, key=len, reverse=True):
                try:
                    if i.count('（') == 2 and i.count('）') == 1:
                        pure_reg = r'.*?(' + i + '.*?）).*?'
                        i = re.match(pure_reg, pure_txt, flags=re.S).group(1)
                except:
                    print('遇到正则问题  只匹配当前情况')
                pure_txt = pure_txt.replace(i, '')
        except:
            with open('error_括号.txt', 'a') as f:
                f.write(item['news_url'] + '\n' + str(ch_) + '\n\n')
        return pure_txt

    def save_to_elasticsearch(self, item, spider, pure_txt):

        pure_txt = self.remove_brackets(item, pure_txt)
        _id = item['_id']
        item.pop('_id')
        item['comment_num'] = item['comment_num'] if item['comment_num'] != '无' else 0
        item['comment_list'] = item['comment_list'] if item['comment_list'] != '无' else 0
        item['news_no'] = item['news_no'] if item['news_no'] != '无' else 0
        item['read_num'] = item['read_num'] if item['read_num'] != '无' else 0
        item['company_count_str'] = item['company_count']
        item['sector'] = item.get('sector') if item.get('sector') is not None else '综合'
        item['order_date'] = int(
            (time.mktime(time.strptime(item['news_date'], '%Y-%m-%d %H:%M:%S')) + 14400 * 2) / 86400)
        item['author_list'] = item.get('author_list')
        item['col_name'] = spider.name
        item['content'] = pure_txt
        item['thumb_num'] = 0  # 自己的点赞数
        item['collection_num'] = 0  # 自己的收藏数
        item['comments_num'] = 0  # 自己的评论数
        item['transmit_num'] = 0  # 自己的转发数
        del item['company_count']

        # 存储 研报 类的文章  结束
        if item.get('sector') == '研报':
            self.block_type = '研报'
            self.es_index = 'hooyuu-research'
            if 'docdetail_' in item['news_url']:
                pdf_url_id = re.match('.*docdetail_(\d+)\.html', item['news_url']).group(1)
                pdf_url = 'http://www.hibor.com.cn/webpdf.asp?&uname=hczqtz&did=' + pdf_url_id + '&degree=1&baogaotype=2&fromtype=21'
                item.update(
                    pdf_url=pdf_url
                )
            self.es.index(index=ES['index_research'], doc_type=ES['doc_type_research'], body=dict(item), id=_id)
            # self.noticer.judge_enter_notice(item, ES['index_research'], doc_type=ES['doc_type_research'])
        # 存储 私募 类的文章  结束
        elif item.get("plate_unique") == "私募":
            self.block_type = '市场点评'
            self.es_index = 'hooyuu-private'

            self.es.index(index=ES['index_private'], doc_type=ES['doc_type'], body=dict(item), id=_id)
            # self.noticer.judge_enter_notice(item, ES['index_private'], doc_type=ES['doc_type'])
        else:
            # 如果是自媒体 传入自媒体的index中
            if item.get('doc_type') == 'wemedia':
                self.block_type = '自媒体'
                self.es_index = 'hooyuu-wemedia'

                item.pop('doc_type')
                item.pop('pic')
                self.es.index(index=ES['index_wemedia'], doc_type=ES['doc_type'], body=dict(item), id=_id)
                # self.noticer.judge_enter_notice(item, ES['index_wemedia'], doc_type=ES['doc_type'])
            # 存储 公告 类的文章  结束
            elif item.get('doc_type') == 'notice':
                self.block_type = '公告'
                self.es_index = 'hooyuu-notice'

                self.es.index(index=ES['index_notice'], doc_type=ES['doc_type_notice'], body=dict(item), id=_id)
                # self.noticer.judge_enter_notice(item, ES['index_notice'], doc_type=ES['doc_type_notice'])
            # 存储 资讯 类的文章  结束
            else:
                self.block_type = '资讯'
                self.es_index = 'hooyuu-total'
                self.es.index(index=ES['index_total'], doc_type=ES['doc_type'], body=dict(item), id=_id)
                # self.noticer.judge_enter_notice(item, ES['index_total'], doc_type=ES['doc_type'])

    # 统计新闻内容 公司数量
    def statistics_company_count(self, item):
        content = item['content'] + item['title']

        wordcont = jieba.cut(content)
        # 计数
        # data = dict(Counter(wordlist))
        # 转成列表 查公司用
        word = list(wordcont)
        companylist = []
        f = open("cninfo_stock.json", encoding='utf-8')
        comps_list = json.load(f)
        for comps in comps_list:
            comps = comps['company']
            for comp in comps:
                companylist.append(comp)
        # 增加知识点 2019/1/8 hyf
        f = open("wemedia_notice.json", encoding='utf-8')
        know_list = json.load(f)
        for knows in know_list.values():
            for know in knows:
                companylist.append(know)
        a = list(set(word).intersection(set(companylist)))

        # 计数
        dict1 = {}
        for company in a:
            num = content.count(company, 0, len(content))
            dict1[company] = num
        # # 技数
        # for keys in a:
        #     num = data[keys]
        #     dict1[keys] = num
        complist = []
        for k, v in dict1.items():
            complist.append([k, v])
        item['company_count'] = complist
        item['content_length'] = len(item['content']) + len(item['title'])
        # 根据doc_type字段判断加入
        spider_types = get_spider_type()
        for s_name, s_type in spider_types.items():
            if self.spider.name == s_name:
                inverted_index = self.client.get_database(s_type)

                self.get_new_db_content(complist, inverted_index, item)

                # 创建新数据库内容
                if complist:
                    for company in complist:
                        company = company[0]
                        from pymongo import HASHED
                        # 获取公司名
                        collection_names = inverted_index.collection_names()
                        company = inverted_index.get_collection(company)
                        if company in collection_names:
                            if company.find({'con_id': item['_id']}).count() == 0:
                                company.insert_one(
                                    {"con_id": item['_id'], 'old_col': self.spider.name, 'con_type': item['sector']})
                        else:
                            company.create_index([("con_id", HASHED)])
                            if company.find({'con_id': item['_id']}).count() == 0:
                                company.insert_one(
                                    {"con_id": item['_id'], 'old_col': self.spider.name, 'con_type': item['sector']})

                # 创建新数据库内容
                self.get_new_db_content(complist, inverted_index, item)

        return item, a

    def get_new_db_content(self, complist, inverted_index, item):
        # 创建新数据库内容
        if complist:
            for company in complist:
                company = company[0]
                from pymongo import HASHED
                # 获取公司名
                collection_names = inverted_index.collection_names()
                company = inverted_index.get_collection(company)
                if company in collection_names:
                    if company.find({'con_id': item['_id']}).count() == 0:
                        company.insert_one(
                            {"con_id": item['_id'], 'old_col': self.spider.name, 'news_date': item['news_date']})
                else:
                    company.create_index([("con_id", HASHED)])
                    if company.find({'con_id': item['_id']}).count() == 0:
                        company.insert_one(
                            {"con_id": item['_id'], 'old_col': self.spider.name, 'news_date': item['news_date']})

    # 根据 文章中的公司列表，去除公司后面的括号
    def delete_content_brackets(self, company_list, content):
        # 去括号
        for company in company_list:
            if company + '(' in content:
                ss = company + '(<.*?>)*(\(.*?\))'
                content_info = re.findall(ss, content, re.S)
                for i in content_info:
                    content = content.replace(i[1], '')
            elif company + '（' in content:
                ss1 = company + '(<.*?>)*(（.*?）)'
                content_info1 = re.findall(ss1, content, re.S)
                for i in content_info1:
                    content = content.replace(i[1], '')
        return content

    # 字符串正则化
    def str_to_regex(self, string):
        string = re.sub('\(', '\(', string)
        string = re.sub('\)', '\)', string)
        string = re.sub(r'\?', '\?', string)
        string = re.sub(r'\|', '\|', string)
        string = re.sub(r'\\', r'\\\\', string)
        return string

    def clear_content_with_tags(self, t, s, url, spider, type=None):

        '''
        :param title: 标题，用于去重
        :param s: 正文
        :param type: 用来判断公告类型，只需要纯文本
        :param spider: 用来获取爬虫名等信息
        :return: 返回带段落标签 不带无用属性的正文
        '''

        # 1. 去掉标题
        filtered_title = self.str_to_regex(t)
        pattern_title = re.compile('<[^>]*?>\s*%s\s*</[^>]*?>' % filtered_title, re.I)
        s = pattern_title.sub('', s)

        # 2. 过滤 script style
        pattern_script_style = re.compile(
            '<\s*script[]*>[\s\S]*?<\s*/\s*script\s*>|<\s*style[^>]*>[\s\S]*?<\s*/\s*styles*>',
            re.I)
        s = pattern_script_style.sub('', s)

        if spider.name not in ['mingdafund_private', 'dingsafund_private', 'ftsfund_private']:
            pattern_tags_content = re.compile(
                '<[^>|^/]*?>\s*?</[^>]*?>|<\s*?table[^>]*?>[\s\S]*?<\s*?/\s*?table[^>]*?>',
                re.I)
            s = pattern_tags_content.sub('', s)

        # 4. 过去无用符号 \n 等
        pattern_symbol = re.compile(r'\n|\r|&nbsp;')
        s = pattern_symbol.sub('', s)

        # 5. 无用标签
        pattern_tags = re.compile(
            r'<!--[\s\S]*?-->|<h-char[\s\S]*?>|</h-char>|<h-inner[\s\S]*?>|</h-inner>|<[^>|^/]*?b[^r|^>|^/]*?>')
        s = pattern_tags.sub('', s)

        # 6.1 去除段落标签的属性
        # 如果type是公告就不走这一步
        paragraph_tags = ['div', 'article', 'p', 'b', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'ul', 'section']

        for p_tag in paragraph_tags:
            s = re.sub(r'<%s.*?>' % p_tag, '<%s>' % p_tag, s)

        # 7.规范化 去除无用空格 和 多余标签
        from lxml import etree
        if s:
            html = etree.HTML(s)
            html_str = etree.tostring(html, encoding="utf-8", pretty_print=True, method="html").decode('utf-8')
            html_str = re.sub('<b>|</b>|<html>|</html>|<body>|</body>|<input.*?>', '', html_str)
        else:
            html_str = ''

        # 用于删除开头的空格
        html_str = html_str.replace('&nbsp;', '').replace('　', '')

        pure_txt = re.sub('<.*?>', '', html_str, flags=re.S)
        try:
            # 留html的文章 将图片取出  补全
            pics = re.findall('<img.*?src="(.*?)"', html_str)
            pics = set(pics)
            for i in pics:
                if i.strip():
                    img_now = parse.urljoin(url, i)
                    html_str = re.sub(i, img_now, html_str, re.S)
        except:
            print('img带括号，无法补全 ,或者是data:image/png 不需要补全')

        return html_str, pure_txt
