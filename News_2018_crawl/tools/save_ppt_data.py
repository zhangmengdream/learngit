# import win32com
# from win32com.client import Dispatch


def ppt_data_parse(item):

    # ppt = win32com.client.Dispatch('PowerPoint.Application')
    # ppt.Visible = 1
    # pptSel = ppt.Presentations.Open(item['local_pdf_url'])
    # fout_cont = ''
    # slide_count = pptSel.Slides.Count
    # for i in range(1, slide_count + 1):
    #     shape_count = pptSel.Slides(i).Shapes.Count
    #     for j in range(1, shape_count + 1):
    #         if pptSel.Slides(i).Shapes(j).HasTextFrame:
    #             s = pptSel.Slides(i).Shapes(j).TextFrame.TextRange.Text.strip()
    #             if len(s) > 0:
    #                 fout_cont += "{}\n".format(s)
    # ppt.Quit()
    with open('ppt的文件内容.txt','a') as f:
        f.write(item['col_name']+'  ' + item['news_url']+'  ' + item['pdf_url']+'\n')
    print('linux暂时不做任何操作 直接返回空字符串')
    fout_cont = ''
    return fout_cont


