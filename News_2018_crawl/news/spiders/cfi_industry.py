# -*- coding: utf-8 -*-
import re
from urllib import parse
import scrapy
from news.items import NewsItem
from news.except_test import try_except
from news.spiders.cfi_base_spider import Cfi_Base_Spider


class Cfi_Industry_Spider(Cfi_Base_Spider):
    name = 'cfi_industry'  # 中财网 产经
    redis_key = 'cfi_industry:start_urls'
    onseturl = 'http://www.cfi.net.cn/'

    def __init__(self, **kwargs):
        super().__init__(self.name, self.redis_key, self.onseturl, **kwargs)


    @try_except()
    def parse(self, response):
        self.num_rest = 0
        item = NewsItem()
        urls = response.xpath('//div[@class="zidiv2"]/a/@href|//*[@id="content"]/center//table[7]//table//a/@href|//div[@class="xinwen"]//a/@href|//div[@class="lanmuye_xinwen"]//a/@href').extract()
        urls = urls[:-2]
        topic = response.xpath('//div[@class="zidiv1"]//text()').extract()
        topic = ''.join(topic)
        item['topic'] = re.sub('\s|&nbsp', '', topic)
        item['catalogue'] = '中财网>产经>' + item['topic']
        if urls:
            for url in urls:
                url = parse.urljoin(response.url, url)
                self.repeat_num(url)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_detail,
                    meta={'item':item,},
                    dont_filter=True
                )
            next_url = response.xpath('//img[@src="pic/nextpage.gif"]/../@href').extract()
            next_url = ''.join(next_url).strip()
            yield scrapy.Request(
                url=parse.urljoin(response.url, next_url),
                callback=self.parse
            )
