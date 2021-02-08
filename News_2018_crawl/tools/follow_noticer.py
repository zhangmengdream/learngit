# coding:utf-8
'''
爬虫文章是否录入 通知表
'''
import datetime
import copy
import pymysql
from pymongo import MongoClient

# 配置部分
# JEESNS_MYSQL_NEWS = {
#     'host': '39.106.89.253',
#     'port': 3306,
#     'root': 'root',
#     'pw': 'VlH2Q.6qqrMa',
#     'hytt_source': 'hytt',
#     'db_name': "jeesns"
# }

# 正式服内网
JEESNS_MYSQL_NEWS = {
    'host': '172.17.21.158',
    'port': 3306,
    'root': 'root',
    'pw': 'VlH2Q.6qqrMa',
    'hytt_source': 'news',
    'db_name': "jeesns_news"
}


# Mongo数据库
Mongo_config = {
    'user': 'root',
    'pwd': 'Jrky8qWXz8muTc5F',
    'host_port': ['dds-2ze9517f74de07541.mongodb.rds.aliyuncs.com:3717',
                  'dds-2ze9517f74de07542.mongodb.rds.aliyuncs.com:3717'],
    'replica_set': 'mgset-13010617',
    'port': 3717,
    'mechanism': 'SCRAM-SHA-1'  # 版本加密方式，测试服MONGODB-CR, 正式服是SCRAM-SHA-1
}



class Noticer:

    def __init__(self):
        self.jn_mysql = pymysql.connect(host=JEESNS_MYSQL['host'], user=JEESNS_MYSQL['root'],
                                        password=JEESNS_MYSQL['pw'], port=JEESNS_MYSQL['port'],
                                        charset='utf8')
        try:
            self.jn_mysql_news = pymysql.connect(host=JEESNS_MYSQL_NEWS['host'], user=JEESNS_MYSQL_NEWS['root'],
                                            password=JEESNS_MYSQL_NEWS['pw'], port=JEESNS_MYSQL_NEWS['port'],
                                            charset='utf8')
        except:
            print("内网本地不连接")
            self.jn_mysql_news = None

        self.mongo = MongoClient(Mongo_config['host_port'], replicaSet=Mongo_config['replica_set'])
        if Mongo_config['user']:
            self.mongo.get_database('admin').authenticate(name=Mongo_config['user'], password=Mongo_config['pwd'],
                                                          mechanism=Mongo_config['mechanism'])

        self.hytt_mongodb = self.mongo.get_database('hytt')
        # 不要显示的板块名称， 用作排除
        self.no_display = ['首页', '军工', '柔性屏', '电力', '养殖', 'A股', "蓝筹", "价值池", "高科技", "高端装备"]

        self.cpname_to_code_dict = self.cpname_to_code_dict()

    # 检查mysql连接是否可用
    def __mysqlMayConnect(self, mysql_client):
        try:
            mysql_client.ping()
        except Exception as e:
            print(e)

    def __del__(self):
        self.jn_mysql.close()
        self.mongo.close()
        if self.jn_mysql_news:
            self.jn_mysql_news.close()


    # 从mongo获取block_knowledge信息 板块-知识点信息
    def get_block_knowledge(self):
        # 包含大分类的原始字典
        self.origin_block_knowledge_words = self.hytt_mongodb.get_collection('data_config').find_one({"name": "block_knowledge"}).get('content')
        # 所有大板块 行业 公司， 的信息集合
        self.allPublicBigBlockAndSmallBlockAndCps = []
        for big_block_content in self.origin_block_knowledge_words.values():
            big_block_info = dict(
                big_block_name=big_block_content['name'],
                small_block_list=[]
            )
            for small_block_content in big_block_content['sub_block'].values():
                small_block_info = dict(
                    small_block_name=small_block_content['name'],
                    companies=[]
                )
                for cps in small_block_content['companies']:
                    small_block_info['companies'].append(cps['name'])
                big_block_info['small_block_list'].append(small_block_info)
            self.allPublicBigBlockAndSmallBlockAndCps.append(big_block_info)

        # 把大分类去掉， 合并成如同 core.json 的格式
        block_knowledge_words = {}
        for big_block_name, big_block_content in self.origin_block_knowledge_words.items():
            # 大板块的 知识点和关键词
            big_block_know_keywords = {big_block_name: []}
            # 如果大板块有格外知识点， 那么加进去
            if big_block_content['big_block_extra_keywords']:
                for know_keywords in big_block_content['big_block_extra_keywords']:
                    # 将知识点作为 词组第一个词
                    know_keywords['keywords'] = [know_keywords["name"]] + know_keywords['keywords']
                    big_block_know_keywords[big_block_name].append(know_keywords['keywords'])

            # 子版块内容 分别加入 大字典block_knowledge_words 和 板块字典big_block_know_keywords
            for small_block_name, small_block_content in big_block_content["sub_block"].items():
                small_block_keywords = []
                # 如果有子版块额外关键词， 那么加入small_block_keywords
                if small_block_content['sub_block_extra_know_keywords']:
                    for know_keywords in small_block_content['sub_block_extra_know_keywords']:
                        # 将知识点作为 词组第一个词
                        know_keywords['keywords'] = [know_keywords["name"]] + know_keywords['keywords']
                        small_block_keywords.append(know_keywords['keywords'])

                # 公司作为 知识点关键词加入 small_block_keywords
                for know_keywords in small_block_content['companies']:
                    # 将知识点作为 词组第一个词
                    know_keywords['keywords'] = [know_keywords["name"]] + know_keywords['keywords']

                    small_block_keywords.append(know_keywords['keywords'])

                # 子版块 额外知识点关键词加入 子版块 和 大板块 名字为key 的字典
                block_knowledge_words[small_block_name] = small_block_keywords
                big_block_know_keywords[big_block_name] += small_block_keywords
            block_knowledge_words.update(big_block_know_keywords)
        self.block_knowledge_words = block_knowledge_words

    def getPublicCompanys(self, have_code=False, all_word=None):
        # 刷新菜单
        self.get_block_knowledge()
        company_dict = {}
        for big_content in self.origin_block_knowledge_words.values():
            if big_content['name'] in set(['首页', '期货']):
                continue
            for small_content in big_content['sub_block'].values():
                for company in small_content['companies']:
                    if company['name'] not in company_dict:
                        if have_code:
                            company_dict[company['name']] = [company['name'], company['code'],
                                                             company.get('pinyin', ''), company.get('short', ''),
                                                             [big_content['name']]]
                        else:
                            company_dict[company['name']] = [company['name'], [big_content['name']]]
                    else:
                        if company['name'] in company_dict and \
                                big_content['name'] not in company_dict[company['name']][-1]:
                            company_dict[company['name']][-1].append(big_content['name'])
        final = list(company_dict.values())
        if not all_word:
            # 去掉板块信息中的A股
            for cp in final:
                for no_word in self.no_display:
                    if no_word in cp[-1]:
                        cp[-1].remove(no_word)

        return final


    # 返回所有 公司->股票 对应字典
    def cpname_to_code_dict(self):
        all_hooyuu_cps = self.getPublicCompanys(have_code=True)
        cp_code_dict = {}
        for cp in all_hooyuu_cps:
            cp_code_dict[cp[0].replace(" ", "")] = cp

        return cp_code_dict

    # 是否加入通知数据库
    def judge_enter_notice(self, item, index_name, doc_type):
        if "非长文" in item.get('title'):
            return
        # "title", "news_date", "referer_web", "author", "news_url", "author_list"
        notice_info_base = {
                "title": item.get('title'),
                "news_date": item.get('news_date'),
                "referer_web": item.get('referer_web'),
                "author": item.get('author'),
                "author_list": item.get('author_list'),
                "author_id": item.get('author_id', ''),
                "news_url": item.get('news_url'),
                "es_id": item.get("_id"),
                "es_index": index_name,
                "doc_type": doc_type,
                "read": 0,
                "create_time": item.get("news_date"),
                "expireAt": datetime.datetime.strptime(item.get("news_date"), "%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=30)
            }

        notice_col = self.hytt_mongodb.get_collection('user_notice')
        notice_lastly_col = self.hytt_mongodb.get_collection('notice_lastly')  # 用户最新通知

        code_list = []
        tmp_code_cp_map = {}
        for x in item.get("company_count"):
            if x[1] >= 2:
                cp_name = x[0]
                code_info = self.cpname_to_code_dict.get(cp_name)
                if code_info:
                    code_list.append(code_info[1])
                    tmp_code_cp_map[code_info[1]] = cp_name

        if code_list:
            # a.1 查询关注了股票 company_list 得用户 hytt
            self.__mysqlMayConnect(self.jn_mysql)
            self.jn_mysql.select_db(JEESNS_MYSQL.get('db_name'))
            cur = self.jn_mysql.cursor()
            sql = "select DISTINCT member_id, is_notice, stockcode from tbl_mystock WHERE stockcode in %s"
            cur.execute(sql, (code_list, ))
            res = cur.fetchall()
            print(res)
            # a.2 录入自选股文章通知信息
            for info in res:
                uid = info[0]
                notice_info = copy.deepcopy(notice_info_base)
                notice_info.update({
                    "uid": int(uid),
                    "notice_type": "optional_stock",
                    "hytt_source": JEESNS_MYSQL.get("hytt_source"),
                    "stock_code": info[2],
                    "stock_name": tmp_code_cp_map[info[2]]
                })
                # notice_col = self.hytt_mongodb.get_collection(
                #     'user_notice_%s_%s' % (JEESNS_MYSQL_NEWS.get("hytt_source"), uid))
                more_notice = info[1]
                if more_notice == 1:  # 强关注
                    notice_col.update_one(notice_info, {'$set': notice_info}, upsert=True)



            if self.jn_mysql_news:
                # a.1 查询关注了股票 company_list 得用户 news 正式服
                self.__mysqlMayConnect(self.jn_mysql_news)
                self.jn_mysql_news.select_db(JEESNS_MYSQL_NEWS.get('db_name'))
                cur = self.jn_mysql_news.cursor()
                sql = "select DISTINCT member_id, is_notice, stockcode from tbl_mystock WHERE stockcode in %s"
                cur.execute(sql, (code_list,))
                res = cur.fetchall()
                # a.2 录入自选股文章通知信息
                for info in res:
                    uid = info[0]
                    notice_info = copy.deepcopy(notice_info_base)
                    notice_info.update({
                        "uid": int(uid),
                        "notice_type": "optional_stock",
                        "hytt_source": JEESNS_MYSQL_NEWS.get("hytt_source"),
                        "stock_code": info[2],
                        "stock_name": tmp_code_cp_map[info[2]]
                    })
                    # notice_col = self.hytt_mongodb.get_collection('user_notice_%s_%s' % (JEESNS_MYSQL_NEWS.get("hytt_source"), uid))
                    more_notice = info[1]
                    if more_notice == 1:  # 强关注
                        notice_col.update_one(notice_info, {'$set': notice_info}, upsert=True)


        # b.1 查询关注了外部作者
        author_list = item.get('author_list')
        author = item.get('author')
        referer_web = item.get('referer_web')
        authors = author_list + [author]

        res = self.hytt_mongodb.get_collection('outside_author').find({"outside_refer": referer_web, "outside_author": {"$in": authors}})
        # b.2 录入外部作者文章通知信息
        for r in res:
            uid = int(r.get('hytt_userid'))
            notice_info = copy.deepcopy(notice_info_base)
            notice_info.update({
                "uid": uid,
                "notice_type": "outside_author",
                "hytt_source": r.get("hytt_source")
            })
            # notice_col = self.hytt_mongodb.get_collection(
            #     'user_notice' % (JEESNS_MYSQL_NEWS.get("hytt_source"), uid))
            more_notice = r.get("notice")
            if more_notice == 1:
                notice_col.update_one(notice_info, {'$set': notice_info}, upsert=True)

            # 更新 notice_lastly_col 的对应文章 count+1
            update_one_query = {"hytt_source": r.get("hytt_source"), "uid": uid,
                                "notice_type": "outside_author", "referer_web": referer_web,
                                "author": r.get("outside_author")}
            update_one = notice_lastly_col.find_one(update_one_query)

            if update_one:
                new_count = update_one.get("new_count", 0) + 1
            else:
                new_count = 1
                notice_info.update({"last_time": ""})
            notice_info.update({"new_count": new_count, "author": r.get("outside_author")})
            notice_lastly_col.update_one(update_one_query, {'$set': notice_info}, upsert=True)


if __name__ == '__main__':
    # 测试数据
    index_name = 'hooyuu-total'
    doc_type = 'news'
    item = {
        "_id": "nxx",
        "order_date": 17879,
        "topic": "财经",
        "content_length": 1003,
        "company_count": [  # 实际es是company_count_str pipline给的数据是company_count
            [
                "德创环保",
                1
            ],
            [
                "亚士创能",
                1
            ],
            [
                "汉嘉设计",
                1
            ],
            [
                "新疆交建",
                2
            ],
            [
                "博天环境",
                1
            ],
            [
                "山东路桥",
                2
            ],
            [
                "科融环境",
                1
            ],
            [
                "雅运股份",
                1
            ],
            [
                "新天科技",
                1
            ]
        ],
        "col_name": "lutouwang_finance",
        "source": "凤凰网财经",
        "simhash_val": "16653075485544235912",
        "author": "老韩",
        "author_list": [
            "老韩"
        ],
        "title": "测试标题6",
        "sector": "理财",
        "pic": ",,",
        "content": "凤凰网财经讯三大指数小幅低开，此后两市震荡走弱，午后持续回落，创业板指跌逾2%，沪指再度失守2600点整数关口，各大板块全线下挫，仅环保板块飘红，超3300股飘绿。截至收盘，沪指收报2593.74点，跌幅1.53%，成交额1237亿；深成指报7629.65点，跌幅2.28%，成交额1682亿；创业板指报1310.46点，跌幅2.82%，成交额514.2亿。12.14指资金进攻路线图图注:今日资金攻击线路图（热点概念与活跃概念,及部分强势品种）涨停:新天科技（雄安新区）=&gt;涨停：科融环境（环保）今日龙虎榜：（注:3日为连续三个交易日内,涨幅偏离值累计达20%的证券）现在的市场太磨人了，越是狂热摔的越惨。大基建一日游行情，虽然周四的日内龙头山东路桥午后再度冲板，但是涨停板仅维持了4分钟就迅速回落，未能维持周四的强势，最终涨幅5.37%。赵老哥所在席位周四买入603万，截至发稿该股并未上榜，按照当日涨幅来看，赵老哥还处于获利状态。汉嘉设计3连板，成唯一基建板块二板股，基建板块整体的回落也给其后续涨停造成了极大的压力。但其如果后期能走强的话，也许可以激活次新和基建题材的活性。亚士创能开盘小幅直线拉升后震荡下行，杀死了一众追高客。歌神所在席位周四买入858万，同时炒股养家所在席位也买入326万，从当日收盘价来看，歌神和炒股养家其实不赚不赔，但按炒股养家的操作风格来看，应该是参与了砸盘。新疆交建大幅低开后维持震荡走势，午后小幅冲高后随即下挫，最终收跌6.01%。欢乐海岸所在席位周四买入1634万。新疆交建虽然未能连板，但是游资对于次新的挖掘却从未停止。雅运股份经过新股上市后的一波一字板后，便开始震荡下行，把前期的涨幅也跌掉了大半，从筹码分布形态来看，属于已经将大部分套牢筹码洗掉，现在呈单峰密集形态。流通市值8.64亿，算是超级小盘股，唯一不足的就是概念太单薄，除了次新这个点外就是分散染料这个概念属性，还是一个周期概念属性，更不是现在热门属性。歌神所在席位买入1600万。虽然环保消息面没有什么实质性的利好，但整个板块却能万绿丛中一点红，可见很是强势，其中科融环保、德创环保以及博天环境多股涨停。昨日龙虎榜回顾：（注:3日为连续三个交易日内,涨幅偏离值累计达20%的证券）注：以上据公开数据整理的资料仅供参考，不构成任何投资性意见！",
        "catalogue": "首页>财经>正文",
        "crawl_date": "2019-05-24 11:21:01",
        "news_url": "http://www.lutouwang.net/detail/19/422/1jurrx38m.html",
        "comment_num": 0,
        "keywords": "基建,一日游,股连板,老哥,买入,山东路桥,封板,分钟",
        "referer_url": "www.lutouwang.net",
        "abstract": "凤凰网财经讯三大指数小幅低开，此后两市震荡走弱，午后持续回落，创业板指跌逾2%，沪指再度失守2600点整数关口，各大板块全线下挫，仅环保板块飘红，超3300股飘绿。截至收盘，沪指收报2593.74点，...",
        "news_date": "2020-05-24 17:43:00",
        "read_num": 0,
        "referer_web": "雪球",
        "tags": "基建,一日游,股连板,老哥,买入,山东路桥,封板,分钟",
        "thumb_num": 0
    }
    noticer = Noticer()
    noticer.judge_enter_notice(item, index_name, doc_type)
