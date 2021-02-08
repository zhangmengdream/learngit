import time
import requests
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.converter import TextConverter, PDFPageAggregator
from pdfminer.layout import LAParams
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfpage import PDFPage
from news.settings import proxies

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
}


# pdf_path = '/home/mikefiles/scrapy_project/News_2018/pdffile/{}.pdf'
pdf_path = r'C:\ZMBAK\News_2018_en\news\pdf_files\{}.pdf'

MAP_SUFFIX = {
    'pdf': 'pdf_data_parse',
    'xls': 'xls_data_parse',
    'xlsx': 'xls_data_parse',
    'pptx': 'pptx_data_parse',

    'docx': 'docx_data_parse',
    'doc': 'doc_data_parse',
    'ppt': 'ppt_data_parse',
}


def get_pdf_url_and_cont(item, headers):

    suffix = 'pdf'
    item['local_pdf_url'] = pdf_path.format(item['_id'], suffix)
    save_file_data(item, headers)

    content = eval(MAP_SUFFIX[suffix] + "(item)")

    return item, content


def proxies_get_url_except(url, headers=headers, timeout=30):
    while True:
        try:
            res = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            if res.status_code == 200:
                return res
        except:

            print('重新get_url：{}'.format(url))

# text_path = r'photo-words.pdf'
def save_file_data(item, headers=headers):
    pdf_res = proxies_get_url_except(item['pdf_url'], headers=headers)
    print('获取pdf等内容时候的状态码：',pdf_res.status_code)
    with open(item['local_pdf_url'], 'wb') as f:
        f.write(pdf_res.content)



def pdf_data_parse(item):
    '''解析PDF文本，并保存到TXT文件中'''
    text_path = item['local_pdf_url']
    # 获取pdf文档
    fp = open(text_path, 'rb')
    # 创建一个与文档相关的解释器
    parser = PDFParser(fp)
    # pdf文档的对象，与解释器连接起来
    try:
        doc = PDFDocument(parser=parser)
    except Exception as e:
        if item.get('col_name') in ['gfqh_futures', 'taipingfund_private','hsbcjt_private','furamc_private']:
            return ''
        print(e)
        print(text_path)
        print(item['pdf_url'])
        print('pdf存储有误 需要重新存储')
        save_file_data(item)
        print('---------------------------------',item)
        return pdf_data_parse(item)


    parser.set_document(doc=doc)

    # 如果是加密pdf，则输入密码
    # doc._initialize_password()
    # 创建pdf资源管理器
    resource = PDFResourceManager()
    # 参数分析器
    laparam = LAParams()
    # 创建一个聚合器
    device = PDFPageAggregator(resource, laparams=laparam)
    # 创建pdf页面解释器
    interpreter = PDFPageInterpreter(resource, device)
    content_datas = ''
    # 获取页面的集合
    for page in PDFPage.get_pages(fp):
        # 使用页面解释器来读取
        interpreter.process_page(page)

        # 使用聚合器来获取内容
        layout = device.get_result()

        for out in layout:
            if hasattr(out, 'get_text'):
                content_datas +=out.get_text()
    # print(content_datas)
    return content_datas


if __name__ == '__main__':
    item = {'local_pdf_url':r'C:\Users\hy_hp\Desktop\考生健康状况报告表.5.pdf',
            'pdf_url':r'http://www.taipingfund.com.cn/news/NewsAttachmentAction.do?method=downloadAttachment&attId=872'
            }
    time1 = time.time()
    cont = pdf_data_parse(item)
    print(cont)
    time2 = time.time()
    print("总共消耗时间为:", time2 - time1)
