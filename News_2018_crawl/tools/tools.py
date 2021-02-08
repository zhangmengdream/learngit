import datetime
import hashlib
import json
import re
from collections import Counter
from urllib import parse
from jieba.analyse import extract_tags
from news.settings import pdf_path, MAP_SUFFIX, proxies
import requests
from tools.save_pdf_data import pdf_data_parse
from tools.save_docx_data import docx_data_parse
from tools.save_xls_data import xls_data_parse
from tools.save_pptx_data import pptx_data_parse
from tools.save_ppt_data import ppt_data_parse
from tools.save_doc_data import doc_data_parse

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
}


def proxies_get_url_except(url, headers=headers, status=[200], timeout=30):
    print(status)
    while True:
        try:
            res = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            if res.status_code in status:
                return res
        except:
            # if 'www.furamc.com' in url:
            #     return requests.get("http://www.furamc.com")
            print('重新get_url：{}'.format(url))


def proxies_post_url_except(url, form_data, headers=headers, timeout=30):
    while True:
        try:
            res = requests.get(url, data=form_data, headers=headers, proxies=proxies, timeout=timeout)
            return res
        except:
            print('重新post：{}'.format(url))


def noproxies_post_url_except(url, form_data, headers=headers, timeout=30):
    while True:
        try:
            res = requests.post(url, data=form_data, headers=headers, timeout=timeout)
            return res
        except:
            print('重新post noproxies：{}'.format(url))


def get_pdf_url_and_cont(item, headers, status=[200]):
    suffix = item['pdf_url'].split('.')[-1]
    if item.get('col_name') in ['taipingfund_private',"piccamc_macro_private","piccamc_invest_private"]:
        suffix = 'pdf'

    item['local_pdf_url'] = pdf_path.format(item['_id'], suffix)
    if suffix == 'png':
        return item, ''
    else:
        save_file_data(item, headers, status)
    content = eval(MAP_SUFFIX[suffix] + "(item)")
    return item, content


def news_id(news_url):
    m = hashlib.md5()
    m.update(bytes(news_url, encoding='utf-8'))
    return m.hexdigest()


def save_file_data(item, headers=headers, status=[200]):
    # if
    print('open')
    pdf_res = proxies_get_url_except(item['pdf_url'], headers=headers, status=status)
    print('end')
    print(pdf_res.status_code)
    # if pdf_res.status_code in status:
    with open(item['local_pdf_url'], 'wb') as f:
        f.write(pdf_res.content)

