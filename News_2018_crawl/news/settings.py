# -*- coding: utf-8 -*-

# Scrapy settings for news project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html
from elasticsearch import Elasticsearch
from pymongo import *
from redis import *

BOT_NAME = 'news'

SPIDER_MODULES = ['news.spiders']
NEWSPIDER_MODULE = 'news.spiders'

HTTPERROR_ALLOWED_CODES = [400, 403, 404, 301]

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5

# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 8
CONCURRENT_REQUESTS_PER_IP = 8

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    # 'scrapy_deltafetch.DeltaFetch': 50,  # 去重增量 结合bsddb3
    # 'scrapy_magicfields.MagicFieldsMiddleware': 51,
    'news.middlewares.NewsSpiderMiddleware': 543,
}

DELTAFETCH_ENABLED = False  # 增量去重开关
# DELTAFETCH_DIR = "" — directory where to store state
# DELTAFETCH_RESET = 1 — reset the state, clearing out all seen requests
# scrapy crawl example -a deltafetch_reset=1

# MAGICFIELDS_ENABLED = True
# MAGIC_FIELDS = {
#     "timestamp": "$time",
#     "spider": "$spider:name",
#     "url": "scraped from $response:url",
#     "domain": "$response:url,r'https?://([\w\.]+)/']",
# }


# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'news.middlewares.MyUserAgentMiddleware': 380,
    'news.middlewares.ProxiesMiddleware': 390,
    # 'news.middlewares.CailianMiddleware': 100

}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'news.pipelines.NewsPipeline': 300,
    # 'news.pipelines.XinghunPipeline': 300,

}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


MY_USER_AGENT = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
]

# 随机获取代理ip
Get_Proxy_url = 'http://192.168.1.6:5555/random'

LOG_LEVEL = 'DEBUG'
# Redis_settings = {
#     'host': '192.168.1.6',
#     'port': 6379,
#     'pwd': 'hyd1001@'
# }

JEESNS_MYSQL = {
    'host': '39.106.89.253',
    'port': 3306,
    'root': 'root',
    'pw': 'VlH2Q.6qqrMa',
    'hytt_source': 'hytt'  # hytt/news/test
}


Mongo_settings = {
    'host_port': '127.0.0.1:27017',
    'port': 27017,
    'mechanism': 'SCRAM-SHA-1'
}

# Mongo_settings ={
#     'user': 'root',
#     'pwd': 'Jrky8qWXz8muTc5F',
#     'host_port': ['dds-2ze9517f74de07541140-pub.mongodb.rds.aliyuncs.com:3717',
#                   'dds-2ze9517f74de07542929-pub.mongodb.rds.aliyuncs.com:3717'],
#     'replica_set': 'mgset-13010617',
#     'port': 3717,
#     'mechanism': 'SCRAM-SHA-1'  # 版本加密方式，测试服MONGODB-CR, 正式服是SCRAM-SHA-1
# }



client = MongoClient(Mongo_settings['host_port'])
if Mongo_settings.get('user'):
    client.get_database('admin').authenticate(name=Mongo_settings['user'],
                                              password=Mongo_settings['pwd'],
                                              mechanism=Mongo_settings['mechanism'])

db = client.get_database('News')
Repeat_db = client.get_database('Repeat')
hooyuu_spider_configure = client.get_database('hooyuu_spider_configure')

Redis_settings = {
    'host': 'localhost',
    'port': 6379,
    # 'pwd': 'hyd1001@'
}

sr = StrictRedis(host=Redis_settings['host'], port=Redis_settings['port'],
                 decode_responses=True)

redis_cli = StrictRedis(db=1, host=Redis_settings['host'], port=Redis_settings['port'],
                      decode_responses=True)

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
# REDIS_PARAMS = {
#     'password': 'hyd1001@',
# }
IP_POOL = [
    'http://223.96.95.229:3128',
    'http://39.107.204.193:8088',
    'http://165.138.225.250:8080',
    'http://114.215.103.121:8081',
    'http://39.107.204.193:8088',

]

PROXY = 'http://27.15.115.136:8088'
# 禁止重定型

# REDIRECT_ENABLED = False


# 重试次数
# RETRY_ENABLED = True
# RETRY_TIMES = 5
# RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 408]


# ------ Scrapy-redis 配置 ------   放在具体的爬虫配置吧, 现在的爬虫分为去重增量(detal-fetch) 和 去重(scrapy-redis)

# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"
# SCHEDULER_PERSIST = True

# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderPriorityQueue"
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderQueue"
# SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderStack"

# ITEM_PIPELINES = {
#     'scrapy_redis.pipelines.RedisPipeline': 400,  # item是否存再redis
# }


spider_configure_name = hooyuu_spider_configure.get_collection('spider_configure_name')
def Merge(dict1, dict2):
    return (dict1.update(dict2))
spider_configure = list(spider_configure_name.find({'is_exist':'存在'}))
WEB_MAP = {}
for i in spider_configure:
    if WEB_MAP:
        Merge(WEB_MAP,{i['spider_name']:i['web_name']})
    else:
        WEB_MAP = {i['spider_name']:i['web_name']}
# print("WEB_MAP:",WEB_MAP)
# 爬取网站名字 跟 爬虫名字对应表
# WEB_MAP = {
#
#     'hooyuu_column': '华研头条-专栏',
#     'abroad_seeking_alpha_news': 'seeking-alpha-news',
#     'abroad_seeking_alpha_stock': 'seeking-alpha-stock',
#     'abroad_seeking_alpha_dividends': 'seeking-alpha-dividends',
#     'abroad_seeking_alpha_podcasts': 'seeking-alpha-podcasts',
#     'abroad_seeking_alpha_market': 'seeking-alpha-market',
#     'abroad_seeking_alpha_efts': 'seeking-alpha-efts',
#     'abroad_seeking_alpha_analysis': 'seeking-alpha-analysis',
#     'abroad_seeking_alpha_information': 'seeking-alpha-information',
#
# }
# elastic_info
# test_server
# ES = {
#     'host': '192.168.1.6',
#     'port': 9200,
#     'auth': 'elastic',
#     'password': 'PMJwu8NvD0XUfbXT40av',
#     # 'index': 'hooyuu-test',
#     # 'doc_type': 'news',
# }

# server_2
ES = {
    'host': '192.168.1.7',
    'port': 9200,
    'auth': 'elastic',
    'password': 'changme',
    'index': 'hooyuu-news',
    'index_total': 'hooyuu-total',
    'index_notice': 'hooyuu-notice',
    'index_research':'hooyuu-research',
    'index_wemedia':'hooyuu-wemedia',
    'index_private':'hooyuu-private',
    'index_hot': 'hooyuu-hot',
    'doc_type': 'news',
    'doc_type_notice': 'notice',
    'doc_type_research': 'research',
}
# ES = {
#     'host':  'es-cn-v641aslv7001bd61f.public.elasticsearch.aliyuncs.com',
#     'port': 9200,
#     'auth': 'elastic',
#     'password': 'NWoTkLrFdPMYRk101',
#     'index': 'hooyuu-news',
#     'index_total': 'hooyuu-total',
#     'index_news_en': 'hooyuu-news-en',
#     'index_notice': 'hooyuu-notice',
#     'index_research': 'hooyuu-research',
#     'index_wemedia': 'hooyuu-wemedia',
#     'index_column': 'hooyuu-column',
#     'index_hot': 'hooyuu-hot',
#     'doc_type': 'news',
#     'doc_type_notice': 'notice',
#     'doc_type_research': 'research',
# }


es = Elasticsearch(
    hosts=[{'host': ES['host'], 'port': ES['port']}],
    http_auth=(ES['auth'], ES['password']))

DB_COL = [{'qq': ['qq_finance', 'qq_insurance', 'qq_money', 'qq_stock', 'qq_stock_xingu']},
          {'sina': ['sina_bank', 'sina_chanjing', 'sina_forex', 'sina_jijin', 'sina_licai', 'sina_stock', 'sina_bond']},
          {'wy': ['wy_finance', 'wy_stock', 'wy_forex', 'wy_business', 'wy_industry']},
          {'ifeng': ['ifeng_stock', 'ifeng_money', 'ifeng_finance']},
          {'sohu': ['sohu_macro', 'sohu_business', 'sohu_stock', 'sohu_lciai', 'sohu_manage']},
          {'stcn': ['stcn_stock', 'stcn_company', 'stcn_finance', 'stcn_data', 'stcn_public']},
          {'zqrb': ['zqrb_finance', 'zqrb_stock', 'zqrb_jrjg', 'zqrb_industry', 'zqrb_fund']},
          {'caijing': ['caijing_macro', 'caijing_stock', 'caijing_industry', 'caijing_money', 'caijing_venture']},
          {'xincai': ['xincai_invest', 'xincai_financing', 'xincai_industry', 'xincai_macro']},
          {'jqka': ['jqka_stock', 'jqka_fund', 'jqka_futures', 'jqka_money', 'jqka_finance', 'jqka_bond', 'jqka_forex',
                    'jqka_National']},
          {'zqzx': ['zqzx_stock', 'zqzx_forex', 'zqzx_money', 'zqzx_fund', 'zqzx_future', 'zqzx_bond']},
          {'ce': ['ce_fund', 'ce_futures', 'ce_stock', 'ce_chanjing', 'ce_finance']},
          ]

CLOSESPIDER_ERRORCOUNT = 1

# 爬虫闲置最大警告次数， 满5次停止爬虫
MAX_IDEL_NUM = 5

xueqiu_futures_list = [
    "光大期货深圳分公司"
]

proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = "H7J72K26U5Q67A8D"
proxyPass = "1E7AE72522B3251B"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
}

pdf_path = r'C:\ZMBAK\News_2018_en\news\pdf_files\{}.{}'

MAP_SUFFIX = {
    'pdf': 'pdf_data_parse',
    'xls': 'xls_data_parse',
    'xlsx': 'xls_data_parse',
    'pptx': 'pptx_data_parse',

    'docx': 'docx_data_parse',
    'doc': 'doc_data_parse',
    'ppt': 'ppt_data_parse',
}