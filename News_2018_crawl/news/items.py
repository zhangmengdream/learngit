# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join

"""
新闻(标题,摘要,正文,url,发布时间,爬从抓取的时间,引用的网站名,引用的源url,作者,已阅读量,已评论量,新闻图片,所在网站的新闻标号,所在网站所属目录)
"""


class ArticleItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class NewsItem(scrapy.Item):
    _id = scrapy.Field()  # id
    title = scrapy.Field()  # 标题
    abstract = scrapy.Field()  # 摘要
    news_date = scrapy.Field()  # 发布时间
    content = scrapy.Field()  # 正文
    news_url = scrapy.Field()  # url
    crawl_date = scrapy.Field()  # 爬从抓取的时间
    referer_web = scrapy.Field()  # 引用的网站名
    referer_url = scrapy.Field()  # 引用的源url
    author = scrapy.Field()  # 作者
    author_id = scrapy.Field()  # 作者
    author_link = scrapy.Field()  # 作者主页
    author_list = scrapy.Field()  # 作者列表
    read_num = scrapy.Field()  # 已阅读量
    comment_num = scrapy.Field()  # 已评论量
    forward_num = scrapy.Field()  # 转发量
    like_num = scrapy.Field()  # 点赞量
    comment_list = scrapy.Field()  # 评论内容列表
    pic = scrapy.Field()  # 新闻图片
    news_no = scrapy.Field()  # 所在网站新闻标号
    topic = scrapy.Field()  # 所在网站所属主题
    catalogue = scrapy.Field()  # 所在网站所属目录
    tags = scrapy.Field()  # 标签
    keywords = scrapy.Field()  # 找到对应的关键词
    source = scrapy.Field()  # 来源网站(网易科技)
    company_count = scrapy.Field()  # 文章出现的公司与其数量
    company_count_str = scrapy.Field()  # 文章出现的公司与其数量
    content_length = scrapy.Field()  # 文章与标题长度
    transpond = scrapy.Field()  # 转发量
    order_date = scrapy.Field()
    col_name = scrapy.Field()
    account = scrapy.Field()

    sector = scrapy.Field()  # 行业板块， 专门板块的新闻都要加上这个字段
    video_url = scrapy.Field()  # 行业板块， 专门板块的新闻都要加上这个字段

    pdf_url = scrapy.Field()  # pdf文件的链接
    local_pdf_url = scrapy.Field()
    company = scrapy.Field()  # 公司名称
    code = scrapy.Field()  # 股票代码
    notice_type = scrapy.Field()  # 公告类型
    doc_type = scrapy.Field()  # 分类"news" 和 "notice"
    stock_block = scrapy.Field()  # 板块名称

    wx_id = scrapy.Field()  # 微信id
    simhash_val = scrapy.Field()  # hash文章

    plate_unique = scrapy.Field()  # 轻易不重复的板块名称  已有[微信(期货),(期货数据)]

    thumb_num = scrapy.Field()  # 自己的点赞数
    collection_num = scrapy.Field()  # 自己的收藏数
    comments_num = scrapy.Field()  # 自己的评论数
    transmit_num = scrapy.Field()  # 自己的转发数

    list_col = scrapy.Field()  # (行业)
    list_cp = scrapy.Field()  # (公司)
    list_label = scrapy.Field()  # (关键词)
