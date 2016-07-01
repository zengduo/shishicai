# coding: utf-8
import os
import sqlite3
from lxml import html
import requests
import time
from mytoolpy import get_perfect


basedir = os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'sqlite_test.sqlite')
SHISHICAI_URL_600 = 'http://zst.cjcp.com.cn/cjwssc/view/ssc_hezhi-hezhi-5-ssc-0-3-3000.html'
SHISHICAI_URL_10 = 'http://www.cailele.com/static/ssc/newlyopenlist.xml'

# 取600条初始数据
def get_init_data():
    print "获取600条初始数据"
    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()

    cur.execute('SELECT qihao FROM shishicai;')
    existlist = [str(item[0]) for item in cur.fetchall()]

    page = requests.get(SHISHICAI_URL_600, timeout=60)
    root = html.fromstring(page.content)
    result = root.xpath('//tbody[@id="pagedata"]/tr')
    param = []

    for item in result:
        qihaovalue = item.xpath('td[2]/text()')[0]
        kjhaovalue = str(item.xpath('td[3]/text()')[0])

        if qihaovalue not in existlist:
            param.append((qihaovalue, kjhaovalue, 0, 0, 0, 0, 0))

    sqlstr = 'INSERT INTO shishicai (qihao, kjhao, maihao, zhong, cankao, zijin, zijinhe) VALUES (?, ?, ?, ?, ?, ?, ?);'
    cur.executemany(sqlstr, param)

    cxn.commit()
    print '更改的数据数量：', cur.rowcount
    cur.close()
    cxn.close()

def set_sum():
    print '生成sum的值'

    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()
    cur.execute('select qihao, kjhao from shishicai;')

    sum_dict = dict()
    for item in cur.fetchall():
        intlist = [int(i) for i in list(item[1])]
        sum_dict[str(item[0])] = sum(intlist)

    cur.close()
    cxn.close()


    return sum_dict

# maihao值为上期5个数字的和/10取余
def set_maihao():
    print '生成maihao的值'

    maihao_dict = dict()
    sum_dict = set_sum()
    min_qihao = min(sum_dict.keys())

    for key, value in sum_dict.items():
        if key == min_qihao:
            maihao_dict[key] = 0
        else:
            pre_qihao = min(get_perfect(key, 1))

            if pre_qihao in sum_dict.keys():
                maihao_value = sum_dict[pre_qihao] % 5
                maihao_dict[key] = maihao_value
            else:
                maihao_dict[key] = 0

    return maihao_dict


# zhong值为计算maihao和当期5个号码相同的个数
def set_zhong():
    print '生成zhong的值'

    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()
    cur.execute('select qihao, kjhao from shishicai;')

    kjhao_dict = {str(qihao): kjhao for qihao, kjhao in cur.fetchall()}

    cur.execute('SELECT qihao, maihao from shishicai;')
    maihao_dict = {str(qihao): maihao for qihao, maihao in cur.fetchall()}

    zhong_dict = dict()
    for key, value in maihao_dict.items():
        zhong_dict[key] = kjhao_dict[key].count(str(value))

    cur.close()
    cxn.close()

    return zhong_dict


# cankao值为zhong当期向前30期的和*10
def set_cankao():
    print '生成cankao的值'

    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()
    cur.execute('select qihao, zhong from shishicai;')

    cankao_dict = dict()
    zhong_dict = {str(qihao): zhong for qihao, zhong in cur.fetchall()}
    for key, value in zhong_dict.items():
        qihao = get_perfect(key, 30)

        if len(qihao) > 30:
            qihao_list = [str(i) for i in qihao]
            cankao = 0
            for item in qihao_list:
                if item in zhong_dict.keys():
                    cankao += zhong_dict[item]
            cankao_dict[key] = cankao * 10

    cur.close()
    cxn.close()

    return cankao_dict


# zijin值为zhong*19.5-5*2
def set_zijin():
    print '生成zijin的值'

    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()
    cur.execute('select qihao, zhong from shishicai;')

    zijin_dict = dict()
    zhong_dict = {str(qihao): zhong for qihao, zhong in cur.fetchall()}
    for key, value in zhong_dict.items():
        zijin_dict[key] = int(value)*19.5 - 5*2

    cur.close()
    cxn.close()

    return zijin_dict


# zijinhe值为当前zijin+上期zijinhe
def set_zijinhe():
    print '生成zijinhe的值'
    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()
    cur.execute('select qihao, zijin from shishicai;')

    zijinhe_dict = dict()
    zijin_dict = {str(qihao): zijin for qihao, zijin in cur.fetchall()}

    for key, value in zijin_dict.items():
        qihao = get_perfect(key, 1)

        if len(qihao) > 1:
            qihao_list = [str(i) for i in qihao]

            zijinhe = 0
            for item in qihao_list:
                if item in zijin_dict.keys():
                    zijinhe += zijin_dict[item]
                else:
                    print item
            zijinhe_dict[key] = zijinhe

    cur.close()
    cxn.close()

    return zijinhe_dict


# 写入数据库
def write_into_db(any_dict, column):
    print 'write %s data' % column
    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()

    sqlstring = 'UPDATE shishicai SET {anycolumn} = {columnvalue} where qihao = {qihao}'
    for key in any_dict.keys():
        executesql = sqlstring.format(anycolumn=column, columnvalue=any_dict[key], qihao=key)
        cur.execute(executesql)
    cur.close()
    cxn.commit()
    cxn.close()

# 取1条最新数据
def get_new_data():
    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()

    cur.execute('SELECT qihao FROM shishicai;')

    existlist = [str(item[0]) for item in cur.fetchall()]

    page = requests.get(SHISHICAI_URL_10, timeout=15)
    root = html.fromstring(page.content)
    result = root.xpath('//row')

    param = []
    for item in result:
        qihaovalue = item.xpath('@expect')[0]

        kjhaovalue = str(item.xpath('@opencode')[0]).replace(',', "")

        if qihaovalue not in existlist:
            param.append((qihaovalue, kjhaovalue, 0, 0, 0, 0, 0))

    sqlstr = 'INSERT INTO shishicai (qihao, kjhao, maihao, zhong, cankao, zijin, zijinhe) VALUES (?, ?, ?, ?, ?, ?, ?);'
    cur.executemany(sqlstr, param)
    cxn.commit()

    print "更新的数据：", cur.rowcount
    cur.close()
    cxn.close()

# 检测数据连贯性
def check_continuous_db():
    cxn = sqlite3.connect('sqlite_test.sqlite')
    cur = cxn.cursor()

    cur.execute('SELECT qihao FROM shishicai;')

    # 获得当前记录的所有期号
    current_set = set([str(item[0]) for item in cur.fetchall()])
    current = max(current_set)
    num = len(current_set)
    print '当前数据总条数为{0}'.format(num)
    # 获得理论下的所有期号
    perfect_set = set(get_perfect(current, num))

    # 利用差集取得丢失的记录期号
    lose_qihao = perfect_set.difference(current_set)

    print '丢失数据{0}条，分别为期号为{1}的时时彩'.format(len(lose_qihao), list(lose_qihao))

    # 数据丢失比例
    lost_proportion = len(lose_qihao)/float(num)

    print '当前数据丢失率为{0}%'.format(lost_proportion*100)

    if lost_proportion > 0.05:
        print "当前数据丢失过多，将重新录入数据"
        get_init_data()
        start_init_db()
    else:
        print "数据较完整，不需要重新录入"


def start_init_db():
    print "开始初始化数据库"
    write_into_db(set_maihao(), 'maihao')
    write_into_db(set_zhong(), 'zhong')
    write_into_db(set_cankao(), 'cankao')
    write_into_db(set_zijin(), 'zijin')
    write_into_db(set_zijinhe(), 'zijinhe')
    print "初始化数据库结束"


# get_init_data()
# start_init_db()
#
#
# # while True:
# #     get_new_data()
# #     start_init_db()
# #     time.sleep(60)
#
# get_new_data()
# start_init_db()

check_continuous_db()
