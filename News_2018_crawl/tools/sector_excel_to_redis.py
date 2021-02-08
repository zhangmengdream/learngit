import hashlib
import xlrd
from redis import *
Redis_settings = {
    'host': 'localhost',
    'port': 6379,
    'pwd': ''
}

# password='hyd1001@'
def save_to_redis():
    # redis_cli = StrictRedis()
    redis_cli = StrictRedis(host=Redis_settings['host'], port=Redis_settings['port'],
                     password=Redis_settings['pwd'])
    readbook = xlrd.open_workbook(r'watch_list.xlsx')
    sheet = readbook.sheet_by_name('新闻')
    nrows = sheet.nrows  # 行
    for i in range(nrows):
        if i > 1:
            link = sheet.cell(i, 2).value
            socter = sheet.cell(i, 4).value
            print(link + '\n' + socter)
            m = hashlib.md5()
            m.update(bytes(link, encoding='utf-8'))
            key_id = m.hexdigest()
            redis_cli.hmset('watch_list', {key_id: socter})

    # 断开链接，减少开销
    redis_cli.connection_pool.disconnect()


if __name__ == '__main__':
    save_to_redis()

