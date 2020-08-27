# -*- coding=utf-8 -*-
# 获取股票增量历史记录

"""
1、查询数据库中STOCK_XXXXXX的所有表，提取出股票代码与最大日期
2、将系统日期与最大日期比对，一头一尾作为提取该股票CSV文件的入参
3、取数，解析并入库（去掉清空表的步骤）
"""

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
from multiprocessing import Pool
import uuid
import os
import numpy as np
import re
import time


# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()


# 查询数据库中STOCK_XXXXXX的所有表，提取出股票代码与最大日期
def getTable():
    # 提取股票代码
    cursor.execute("show tables")
    curesult=cursor.fetchall()
    table_re=re.compile(r'stock_\d+') 
    tableNameList = []
    for i in curesult:
        for key in i:
           # print(i[key])
            if table_re.match(i[key]):
                tableNameList.append(i[key])
    #print(tableNameList)
    # print(tableName)
    # code = [ "STOCK_"+str(i) for i in code]

    # 提取最大日期
    codeList = []
    maxDateList = []
    for i in tableNameList:
        sqlStr = "SELECT DATE_FORMAT(max(DATE),'%Y%m%d') as maxdate FROM "+ i
        print(sqlStr)
        cursor.execute(sqlStr)
 
        maxDate = cursor.fetchone()["maxdate"]
        #print(maxDate)
        codeList.append(i.replace("stock_", ""))
        maxDateList.append(maxDate)

    # 将两个list合为字典
    dictionary = dict(zip(codeList, maxDateList))
    return dictionary


# 将系统日期与最大日期比对，一头一尾作为提取该股票CSV文件的入参
def getStockDataInc(code, maxDate, systemTime):
    if re.match('6', code):
        url = "http://quotes.money.163.com/service/chddata.html?code=0" + str(code) + \
              "&start=" + str(maxDate)+ "&end=" + systemTime
    else:
        url = "http://quotes.money.163.com/service/chddata.html?code=1" + str(code) + \
              "&start=" + str(maxDate) + "&end=" + systemTime

    return code, url

# 通过网易财经获取增量数据的CSV文件
def getCSV(code, url):
    fordername = 'AllStockDataInc'
    filename = str(code) + '.CSV'
    if not os.path.isdir(fordername):
        #print("mkdir")
        os.mkdir(fordername)

    with request.urlopen(url) as web:
        # 为防止编码错误，使用二进制写文件模式
        with open(fordername+os.path.sep+filename, 'wb') as outfile:
            outfile.write(web.read())

    #print("save")
    saveInDB(code)


# 将获取的数据入库
def saveInDB(code):
    # 解析CSV文件并数据清洗
    print("start save",code)
    fordername = 'AllStockDataInc'
    filename = str(code) + '.CSV'
    df = pd.read_csv(fordername +os.path.sep+ filename, encoding='gbk')
    df.rename(columns={u'日期': 'sdate', u'股票代码': 'code', u'名称': 'name', u'收盘价': 'close', u'最高价': 'high',
                       u'最低价': 'low', u'开盘价': 'open', u'前收盘': 'y_close', u'涨跌额': 'p_change', u'涨跌幅': 'p_change_rate',
                       u'换手率': 'turnover', u'成交量': 'volume', u'成交金额': 'amount', u'总市值': 'marketcap',
                       u'流通市值': 'famc', u'成交笔数': 'zbs'}, inplace=True)
    df['code'] = code
    df['classify'] = get_type(code)
    dfLen = len(df)
    #print("dfLen")
    #print(dfLen)
    df['uuid'] = [uuid.uuid1() for l in range(0, dfLen)]  # 添加uuid

    # 处理None
    df = df.replace('None', 0)

    # 转浮点数
    df['amount'] = [as_num(x) for x in df['amount']]
    df['marketcap'] = [as_num(y) for y in df['marketcap']]
    df['famc'] = [as_num(z) for z in df['famc']]
    #print(df)
    print("now insert to database")
    try:
        for k in range(0, dfLen) :
            df2 = df[k:k + 1]
            print(type(code)) 
            #print(df2)
            #print("sql")
            sql = "insert into stock_"+code+"(uuid, `DATE`, code, name, classify, open, close, high, low,"\
              "volume, amount, y_close, p_change, p_change_rate, turnover, marketcap, famc, zbs) " \
              "values('%s', '%s', '%s', '%s', '%s', '%.4f', '%.4f', '%.4f', '%.4f', " \
              "'%.4f', '%.4f' , '%.4f', '%.4f','%.4f', '%.4f','%.4f','%.4f','%.4f')"  % ( str(list(df2['uuid'])[0]),
                           str(list(df2['sdate'])[0]),
                           str(list(df2['code'])[0]),
                           str(list(df2['name'])[0]),
                           str(list(df2['classify'])[0]),
                           round(float(df2['open']), 4) ,
                           round(float(df2['close']), 4) ,
                           round(float(df2['high']), 4) ,
                           round(float(df2['low']), 4) ,
                           round(float(df2['volume']), 4) ,
                           round(float(df2['amount']), 4) ,
                           round(float(df2['y_close']), 4) ,
                           round(float(df2['p_change']), 4) ,
                           round(float(df2['p_change_rate']), 4) ,
                           round(float(df2['turnover']), 4) ,
                           round(float(df2['marketcap']), 4) ,
                           round(float(df2['famc']), 4) ,
                           round(float(df2['zbs']), 4)
                         )
            #print(sql)
            #c插入之前先判断，是否存在记录，如果存在，则不更新
            cursor.execute("select * from stock_"+code+" where `date`='%s'" % str(list(df2['sdate'])[0]))
            if cursor.rowcount >0:
                cursor.execute("delete from stock_"+code+" where `date`='%s'" % str(list(df2['sdate'])[0]))
            cursor.execute(sql)
        cursor.execute("commit")
        print("insert Ok" )
    except Exception as e:
        print(e)


# 获取股票分类信息
def get_type(code):
    code_pre = code[0:3]
    switcher = {
        '300': "创业板",
        '600': "沪市A股",
        '601': "沪市A股",
        '602': "沪市A股",
        '900': "沪市B股",
        '000': "深市A股",
        '002': "中小板",
        '200': "深市B股"
    }
    return switcher.get(code_pre, '未知')


# 将科学记数法化为浮点数
def as_num(x):
    y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
    return(y)
def return_valid_code_url():

    dictionary = getTable()
    # 获取系统时间
    systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
    # print(dictionary)
    for key in dictionary:
        #try:
        code, url = getStockDataInc(key, dictionary[key], systemTime)
        # 过滤掉全量数据中未入库的数据
        if 'None' not in url:
            print(code, systemTime)
            yield code,url


def main():
        #except Exception as e:
        #    pass
        #    print(e
   #获取下载链接和股票代码 
    valid_code_url=return_valid_code_url()
    pool=Pool(processes=3)
    for c,u in valid_code_url:
        pool.apply_async(getCSV,(c,u,))

    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
    # print()
# -*- coding=utf-8 -*-
# 获取股票增量历史记录
