import xlrd


# 获取爬虫相应类型
def get_spider_type():
    spider_type_dict = {}
    readbook = xlrd.open_workbook(r'../tools/news_type.xlsx')
    sheet = readbook.sheet_by_name('all_spider')
    nrows = sheet.nrows  # 行
    for i in range(nrows):
        i = sheet.row_values(i)
        spider_type_dict[i[0]] = i[1]
    return spider_type_dict
