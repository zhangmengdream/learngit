# -*- coding: utf-8 -*-
from scrapy.exceptions import CloseSpider
import functools
from news.notices import to_wx_error_info
from news.settings import db, redis_cli
import datetime
import re


def try_except():
    def decorator(func):
        @functools.wraps(func)
        def handle_problems(self, *args, **kwargs):
            now_func = re.match('.*?\.(.*?) .*', str(func)).group(1)
            # 稍后检查是否需要此判断
            redis_cli.set(self.__class__.name + '_now_func', now_func)
            try:
                a = func(self, *args, **kwargs)
                for item in a:
                    yield item

            except Exception:
                if self.has_finished == False:
                    dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    col = db.get_collection(self.__class__.name)
                    start_num = redis_cli.get(self.__class__.name + '_start_num_count')
                    end_num = col.count()
                    last_info = list(col.find({'last_content': {'$exists': True}}, {'last_content': 1}))
                    last_count = last_info[0]['last_content'] if last_info else dt
                    if end_num - int(start_num) > 0:
                        last_count = dt
                    last_grasp_time = {
                        'last_content': last_count,
                        'last_time': dt,
                        'last_err_time': dt,
                        'spider_state': '爬虫报错'
                    }
                    # 如果已经存在字段  更新
                    if col.find({'last_content': {'$exists': True}}).count():
                        col.update({'last_content': {'$exists': True}}, last_grasp_time)  # 更新
                    else:
                        col.insert_one(last_grasp_time)

                    # 发送错误报告给微信  如果有上级
                    error_short = '网页解析错误\n'
                    if args:
                        error_short = '网页解析错误\n' + args[0].url + '\n' + '上一级链接' + str(
                            args[0].request.headers.get('Referer'))
                    to_wx_error_info(app_cate='新闻爬虫', app_name=self.__class__.name, error_level=2,
                                     error_short=error_short)
                raise CloseSpider

        return handle_problems

    return decorator
