import xlrd,requests,time
from news.settings import proxies


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
}


def proxies_get_url_except(url, headers=headers, timeout=30):
    while True:
        try:
            res = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            if res.status_code == 200:
                return res
        except:
            print('重新get_url：{}'.format(url))

def save_file_data(item, headers=headers):
    pdf_res = proxies_get_url_except(item['pdf_url'], headers=headers)
    print('获取pdf等内容时候的状态码：',pdf_res.status_code)

    with open(item['local_pdf_url'], 'wb') as f:
        f.write(pdf_res.content)

# 若还有同样报错  查看是否可以再tools里面一并处理
def xls_data_parse(item):
    try:
        data = xlrd.open_workbook(item['local_pdf_url'], encoding_override='utf-8')
    except:
        print('xls存储有误 需要重新存储')
        save_file_data(item)
        print(item['local_pdf_url'])
        return xls_data_parse(item)


    contents = ''
    for i in range(2):
        table = data.sheets()[i]  # 选定表
        nrows = table.nrows  # 获取行号
        for i in range(1, nrows):  # 第0行为表头
            alldata = table.row_values(i)  # 循环输出excel表中每一行，即所有数据
            alldata = ''.join(alldata)
            contents += alldata
    print(contents)
    return contents


if __name__ == '__main__':
    time1 = time.time()
    item = {"local_pdf_url": r'C:\Users\hy_hp\Downloads\1597105352098056284.xlsx'}
    contents = xls_data_parse(item)
    print(contents)
    time2 = time.time()
    print("总共消耗时间为:", time2 - time1)
