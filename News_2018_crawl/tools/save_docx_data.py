import docx

bad_cols = []
def docx_data_parse(item):
    content = ''
    try:
        file = docx.Document(item['local_pdf_url'])
    except:
        if item.get('col_name') in ['furamc_private']:
            return ''
    for para in file.paragraphs:
        content+=para.text
    return content.strip()


if __name__ == '__main__':
    item = {"local_pdf_url": r"C:\ZMBAK\News_2018\my_test\futures_spider\tools\cb57f573a2646b4e5eb5ed18ac571726.docx"}
    all_datas = docx_data_parse(item)
    print(all_datas)
