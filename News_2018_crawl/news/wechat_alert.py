#!/usr/bin/env python
# -*- coding:utf-8 -*-
import requests
import json
import logging  # 引入logging模块
import os.path
import time

'''
企业微信通知者，类
'''


class WechatNotice(object):

    def __init__(self):
        """
        企业微信注册链接 https://work.weixin.qq.com/wework_admin/register_wx?from=myhome
        corpid 注册完企业微信后的企业ID
        corpsecret 应用与小程序栏中自己创建一个应用得到的 secret

        """
        self.corpid = 'ww183be7778960df23'
        self.corpsecret = 'DBMedk2fOWDjI1ZZMsQkFKkpRJvziXFWl2qwHdlrccU'

    def get_token(self):
        """
        根据API获取token
        """
        url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
        values = {
            'corpid': self.corpid,
            'corpsecret': self.corpsecret,
        }
        req = requests.post(url, params=values)
        data = json.loads(req.text)
        return data["access_token"]

    def send_msg(self, msg):

        """
        调用企业微信发送信息API, 参考链接 https://work.weixin.qq.com/api/doc#10167
        参考链接上有详细的参数说明
        touser: 默认为@all
        agentid: 创建应用时的 agentid
        msg: 程序报错时需要报警提示,在爬虫程序中传递过来
        需要在所创建的应用中设置可见范围,可自己创建群组
        在程序中调用方式 WechatNotice().send_msg(msg)
        """

        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + self.get_token()
        data = {
            # "touser": "@all",
            # "touser": "HanYuFei|67cda29e8ba80550fe0c3eef01d21129",
            # "touser": "LiDongYa",
            "touser": "67cda29e8ba80550fe0c3eef01d21129",
            "msgtype": "text",
            "agentid": 1000002,
            "text": {
                "content": msg
            },
            "safe": 0
        }
        try:
            res = requests.post(url, json.dumps(data))
            result = json.loads(res.content.decode())
            if result["errmsg"] != "ok":
                print('消息无法发送')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    wx_notice = WechatNotice()
    wx_notice.send_msg('新浪爬虫解析错误')
