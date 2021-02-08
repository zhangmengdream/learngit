# -*- coding: utf-8 -*-
import time
import re
import hashlib
from urllib import parse
import scrapy
from news.base_spider import Base_Spider
from news.except_test import try_except


class Cfi_Base_Spider(Base_Spider):

    @try_except()
    def parse_detail(self, response):
        self.num_rest = 0
        item = response.meta['item']
        m = hashlib.md5()
        m.update(bytes(response.url, encoding='utf-8'))
        item['_id'] = m.hexdigest()
        item['news_url'] = response.url
        item['sector'] = self.sector
        item['referer_web'] = '中财网'
        item['referer_url'] = 'cfi.cn'
        item['read_num'] = '无'
        item['comment_num'] = '无'
        item['comment_list'] = '无'
        item['news_no'] = '无'
        item['author'] = '无'
        item['keywords'] = '无'
        item['tags'] = item['keywords']
        # 抓取时间
        nt = time.localtime(int(time.time()))
        item['crawl_date'] = time.strftime('%Y-%m-%d %H:%M:%S', nt)
        item['source'] = '中财网'
        if self.name == 'cfi_future':
            item['plate_unique'] = '期货'
        if response.xpath('//div[@id="tdcontent"]/h1//text()').extract():
            content1 = response.xpath('//div[@id="tdcontent"]//text()').extract()
            content = response.xpath('//div[@id="tdcontent"]').extract()
            content = ''.join(content)
            if content:
                item['content'] = content
            else:
                return
            pic = response.xpath('//div[@id="tdcontent"]//img/@src').extract()
            item['pic'] = ','.join(pic)
            item['abstract'] = ''.join(content1)[:150]
            title = response.xpath('//div[@id="tdcontent"]/h1//text()').extract()
            item['title'] = ''.join(title).strip()
            info = response.xpath('//div[@id="tdcontent"]//table//text()').extract()
            info = re.sub('\s|&nbsp', '', ''.join(info))
            info = re.match('.*?时间：(.*?)中财网.*', info)
            news_date = info.group(1)
            news_date = re.sub('年|月', '-', news_date)
            news_date = re.sub('日', ' ', news_date)
            if re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',news_date):
                item['news_date'] = news_date
            else:
                item['news_date'] = news_date + '00:00:00'
            next_url = response.xpath('//a[contains(.,"下一页")]/@href').extract()
            if next_url:
                yield scrapy.Request(
                    url=parse.urljoin(response.url, next_url[0]),
                    callback=self.next_con,
                    meta={'item': item, }
                    )
            else:
                yield item

    @try_except()
    def next_con(self,response):
        self.num_rest = 0
        item = response.meta['item']
        content = response.xpath('//div[@id="tdcontent"]').extract()
        content = ''.join(content)
        item['content'] = item['content'] + content
        pic = response.xpath('//div[@id="tdcontent"]//img/@src').extract()
        if pic:
            item['pic'] = item['pic'] + ',' + ','.join(pic)
        else:
            item['pic'] = item['pic']
        next_url = response.xpath('//a[contains(.,"下一页")]/@href').extract()
        if next_url:
            yield scrapy.Request(
                url=parse.urljoin(response.url, next_url[0]),
                callback=self.next_con,
                meta={'item': item, }
            )
        else:
            yield item
