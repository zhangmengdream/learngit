# -*- coding: utf-8 -*-
import subprocess


def doc_data_parse(item):
    word = item['local_pdf_url']
    output = subprocess.check_output(['antiword',word])
    return output.decode()


if __name__ == '__main__':
    item = {"local_pdf_url": r"C:\Users\hy_hp\Downloads\2014-08-07-095056-参会回执（私募）.doc"}
    all_datas = doc_data_parse(item)
    print(all_datas)
