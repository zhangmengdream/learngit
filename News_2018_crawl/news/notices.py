from .wechat_alert import WechatNotice
import traceback
import logging
import time
import os
import datetime
from redis import *
from .settings import Redis_settings

wx_noticer = WechatNotice()

logger_wx = logging.getLogger()
logger_wx.setLevel(logging.ERROR)
# 创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d', time.localtime(time.time()))
log_path = os.path.join(os.getcwd(), 'Logs')
print(log_path)
log_file = os.path.join(log_path, rq + '.log')
fh = logging.FileHandler(log_file, mode='w')
fh.setLevel(logging.ERROR)
# 定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger_wx.addHandler(fh)

# 常见错误
ERROR = {
    # 爬虫解析错误
    'paser_error': '响应内容解析错误',
    # 输入存储数据库错误
    'to_db_error': '向数据库传输数据错误，请检查程序 或 数据库运行状态',
    # 输入搜索引擎错误
    'to_es_error': '向elastic传输数据错误，请检查程序 或 elastic运行状态',
}

# 错误级别
'''
5 Debug  调试的信息，忽略
4 Info   需要被记录下来的信息，回头人工处理
3 Warn   出现一些不影响后续的异常，应该在打开网页的时候被凸显出来，但是并不用发微信
2 Error  服务执行中出错了导致后续没法正确处理了，立刻发微信
1 Fatal  服务挂了，立刻发微信
'''


# 响应解析错误 目前作用是跟redis数据库交互错误信息， 发送微信数据， 再pipeline爬虫结束时，看是否有新增错误，而发送报错信息
def to_wx_error_info(app_cate, app_name, error_level, error_short, error_detail=None):
    '''
    :param app_cate: 应用类别
    :param app_name: 应用名称
    :param error_level: 错误级别 （传入序号，序号越小越重要）
    :param error_short: 错误简述
    :param error_detail: 错误所在处，具体信息，通常为 traceback 信息
    :return:
    '''

    # 具体位置/错误信息
    trace_msg = traceback.format_exc()

    # 错误级别 序号和名称对应xxxxxxxxxxxxxxxxxx
    level = {
        5: 'Debug',
        4: 'Info',
        3: 'Warn',
        2: 'Error',
        1: 'Fatal',
    }

    msg = '[信息来源] 本地测试 ' + '\n' \
          + '[应用类别] ' + app_cate + '\n' \
          + '[应用] ' + app_name + '\n' \
          + '[时间] ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n' \
          + '[错误级别] ' + level[error_level] + '\n' \
          + '[错误类型] ' + error_short + '\n' \
          + '[错误描述] ' + trace_msg if error_detail is None else error_detail

    # 微信企业端发送错误信息
    wx_noticer.send_msg(msg)

    logger_wx.error(msg)

    # 将错误记录在redis, 选择数据库 1 作为爬虫状态记录
    try:
        redis_cli = StrictRedis(db=1, host=Redis_settings['host'], port=Redis_settings['port'],
                                password=Redis_settings['pwd'], decode_responses=True)
    except Exception:
        wx_noticer.send_msg('Redis 连接失败\n' + trace_msg)

    print(redis_cli.hget(app_name, 'error_times'))
    print('---------------------------')
    error_times = eval(redis_cli.hget(app_name, 'error_times')) + 1
    fisrt_error = eval(redis_cli.hget(app_name, 'fisrt_error'))
    # 如果first_error为0， 那么将第一条错误信息加进，本次爬虫之后的错误不加入
    if fisrt_error == 0:
        redis_cli.hmset(app_name, {'error_info': msg})
        redis_cli.hmset(app_name, {'fisrt_error': 1})

    redis_cli.hmset(app_name, {'error_times': error_times})
    # 断开链接，减少开销
    redis_cli.connection_pool.disconnect()


if __name__ == '__main__':
    to_wx_error_info(app_cate='爬虫', app_name='新浪', error_level=2, error_short='网页解析错误')
