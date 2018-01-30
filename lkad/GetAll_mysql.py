# -*- coding=utf-8 -*-
# 获取全量股票历史记录

import urllib
import urllib.request
from urllib import request
from bs4 import BeautifulSoup
import tushare as ts
import uuid
from sqlalchemy import create_engine
import cx_Oracle as cxo
import configparser
import pandas as pd
import re
import time
from multiprocessing import Pool
import uuid
import os


# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()


# 列出所有股票代码及名称，并存入dictionary
def listStock(cursor):
    dict1 = {}  #存放开头为6的股票代码及访问链接
    cursor.execute("select code from stock_basics where code like '6%'")
    pdata1 = cursor.fetchall()
    for i in pdata1:
        #print(i)
        #print(i["code"])
        dict1[i["code"]] = 'http://quotes.money.163.com/service/chddata.html?code=0' + str(i["code"]) + '&start=19900101&end=20171231'

    dict2 = {}  # 存放开头不为6的股票代码及访问链接
    cursor.execute("select code from stock_basics where code not like '6%'")
    pdata2 = cursor.fetchall()
    for i in pdata2:
        dict2[i["code"]] = 'http://quotes.money.163.com/service/chddata.html?code=1' + str(i["code"]) + '&start=19900101&end=20171231'

    # 合并两个字典
    dict3 = dict(dict1, **dict2)
    return dict3


# 通过网易财经获取全量数据的CSV文件
def getCSV(code, url):
    fordername = 'AllStockData'
    filename = str(code) + '.CSV'
    if not os.path.isdir(fordername):
        print("mkdir")
        os.mkdir(fordername)
    with request.urlopen(url) as web:
        # 为防止编码错误，使用二进制写文件模式
        print(web)
        with open(fordername+os.path.sep+filename, 'wb') as outfile:
            outfile.write(web.read())
            print("write OK "+str(code))
    #print("id1")
    
    #print(id(cursor))
    import common.GetMysqlConn as conn2
    cursor=conn2.getConfig() 
    #print("Id2")
   # print(id(cursor))
    saveInDB(code,cursor)
    
    print("一只股票入库完毕")
    # 删除CSV文件
    os.remove(fordername+filename)

# 将获取的数据入库
def saveInDB(code,cursor):
    # 建表
    if not is_table_exist(code,cursor):  # 如果表不存在，先创建表
        print(cursor,"not_exist")
        create_table(code,cursor)  # 如果表不存在，先建表
    else:  # 存在则截断
        print(cursor,"exist")
        cursor.execute("truncate table stock_" + code)
        cursor.execute("commit")

    # 解析CSV文件并数据清洗
    fordername = 'AllStockData'+os.path.sep
    filename = str(code) + '.CSV'
    df = pd.read_csv(fordername+filename, encoding='gbk')
    df.rename(columns={u'日期': 'date', u'股票代码': 'code', u'名称': 'name', u'收盘价': 'close', u'最高价': 'high',
                       u'最低价': 'low', u'开盘价': 'open', u'前收盘': 'y_close', u'涨跌额': 'p_change', u'涨跌幅': 'p_change_rate',
                       u'换手率': 'turnover', u'成交量': 'volume', u'成交金额': 'amount', u'总市值': 'marketcap',
                       u'流通市值': 'famc', u'成交笔数': 'zbs'}, inplace=True)
    df['code'] = code
    df['classify'] = get_type(code)
    dfLen = len(df)
    uuidList = []  # 添加uuid
    for l in range(0, dfLen):
        uuidList.append(uuid.uuid1())
    df['uuid'] = uuidList

    #处理None
    df = df.replace('None',0)
    # 转浮点数
    amountList = []
    for x in df['amount']:
        amountList.append(as_num(x))
    df['amount'] = amountList

    marketcapList = []
    for y in df['marketcap']:
        marketcapList.append(as_num(y))
    df['marketcap'] = marketcapList

    famcList = []
    for z in df['famc']:
        famcList.append(as_num(z))
    df['famc'] = famcList

    # 入库
    try:
        for k in range(0, dfLen):
            df2 = df[k:k + 1]
            #print(df2)
            #print("sql pinjie")
#            df2.replace('None',0)
            #print(df2)
            #print(df2['zbs'])
            #print(type(df2['zbs']))
           # print(df2['zds'].dtype)
#            #    print("6464646464664")
#                           round(float(df2['open']), 4) if df2['open'] != "None" else 0 ,
#                           round(float(df2['close']), 4) if df2['close'] != "None" else 0.0000 ,
#                           round(float(df2['high']), 4) if df2['high'] != "None" else 0.0000 ,
#                           round(float(df2['low']), 4) if df2['low'] != "None" else 0.0000 ,
#                           round(float(df2['volume']), 4) if df2['volume'] != "None" else 0.0000 ,
#                           round(float(df2['amount']), 4) if df2['amount'] != "None" else 0.0000 ,
#                           round(float(df2['y_close']), 4) if df2['y_close'] != "None" else 0.0000 ,
#                           round(float(df2['p_change']), 4) if df2['p_change'] != "None" else 0.0000 ,
#                           round(float(df2['p_change_rate']), 4) if df2['p_change_rate'] != "None" else 0.0000 ,
#                           round(float(df2['turnover']), 4) if df2['turnover'] != "None" else 0.0000 ,
#                           round(float(df2['marketcap']), 4) if df2['marketcap'] != "None" else 0.0000 ,
            sql = "insert into stock_"+str(code)+"(uuid, `DATE`, code, name, classify, open, close, high, low," \
              "volume, amount, y_close, p_change, p_change_rate, turnover, marketcap, famc, zbs) " \
              "values('%s', '%s', '%s', '%s', '%s', '%.4f', '%.4f', '%.4f', '%.4f', " \
              "'%.4f', '%.4f' , '%.4f', '%.4f','%.4f', '%.4f','%.4f','%.4f','%.4f')"  % ( str(list(df2['uuid'])[0]),
                           str(list(df2['date'])[0]),
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
#                           str(list(df2['date'])[0]),
#                           str(list(df2['code'])[0]),
#                           str(list(df2['name'])[0]),
#                           str(list(df2['classify'])[0]),
#                           round(float(df2['open']), 4) ,
#                           round(float(df2['close']), 4) if df2['close'] != "None" else 0.0000 ,
#                           round(float(df2['high']), 4) if df2['high'] != "None" else 0.0000 ,
#                           round(float(df2['low']), 4) if df2['low'] != "None" else 0.0000 ,
#                           round(float(df2['volume']), 4) if df2['volume'] != "None" else 0.0000 ,
#                           round(float(df2['amount']), 4) if df2['amount'] != "None" else 0.0000 ,
#                           round(float(df2['y_close']), 4) if df2['y_close'] != "None" else 0.0000 ,
#                           round(float(df2['p_change']), 4) if df2['p_change'] != "None" else 0.0000 ,
#                           round(float(df2['p_change_rate']), 4) if df2['p_change_rate'] != "None" else 0.0000 ,
#                           round(float(df2['turnover']), 4) if df2['turnover'] != "None" else 0.0000 ,
#                           round(float(df2['marketcap']), 4) if df2['marketcap'] != "None" else 0.0000 ,
#                           round(float(df2['famc']), 4) if df2['famc'] != "None" else 0.0000 ,
#                      
           # print(sql)
            cursor.execute(  sql)
        cursor.execute("commit")
    except Exception as e:
        pass
        print(e)

    #cursor.execute("commit")
    


# 将科学记数法化为浮点数
def as_num(x):
    y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
    return(y)


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

# 建表
def create_table(code,cursor):
    sql = "CREATE TABLE stock_" + code + """
    (
            UUID VARCHAR(80) PRIMARY KEY,
            `DATE` DATE,
            CODE VARCHAR(20),
            NAME VARCHAR(80),
            CLASSIFY VARCHAR(80),
            OPEN decimal(20, 4),
            CLOSE decimal(20, 4),
            HIGH decimal(20, 4),
            LOW decimal(20, 4),
            VOLUME decimal(20, 4),
            AMOUNT decimal(20, 4),
            Y_CLOSE decimal(20, 4),
            P_CHANGE decimal(20, 4),
            P_CHANGE_RATE decimal(20, 4),
            TURNOVER decimal(20, 4),
            MARKETCAP decimal(30, 4),
            FAMC decimal(30, 4),
            ZBS decimal(20, 4)
        )
    """
    print(sql)
    cursor.execute(sql)
    cursor.execute('commit')
    # 添加注释
#    comments = ["COMMENT ON TABLE STOCK_" + code + " IS '" + code + "'",  # 表注释
#                "COMMENT ON COLUMN STOCK_" + code + ".UUID IS 'UUID'",
#                "COMMENT ON COLUMN STOCK_" + code + ".\"DATE\" IS '日期'",
#                "COMMENT ON COLUMN STOCK_" + code + ".CODE IS '股票代码'",
#                "COMMENT ON COLUMN STOCK_" + code + ".NAME IS '股票名称'",
#                "COMMENT ON COLUMN STOCK_" + code + ".CLASSIFY IS '类别'",
#                "COMMENT ON COLUMN STOCK_" + code + ".OPEN IS '开盘价'",
#                "COMMENT ON COLUMN STOCK_" + code + ".CLOSE IS '收盘价'",
#                "COMMENT ON COLUMN STOCK_" + code + ".HIGH IS '最高价'",
#                "COMMENT ON COLUMN STOCK_" + code + ".LOW IS '最低价'",
#                "COMMENT ON COLUMN STOCK_" + code + ".VOLUME IS '成交量'",
#                "COMMENT ON COLUMN STOCK_" + code + ".AMOUNT IS '成交金额'",
#                "COMMENT ON COLUMN STOCK_" + code + ".Y_CLOSE IS '昨收盘'",
#                "COMMENT ON COLUMN STOCK_" + code + ".P_CHANGE IS '涨跌额'",
#                "COMMENT ON COLUMN STOCK_" + code + ".P_CHANGE_RATE IS '涨跌幅'",
#                "COMMENT ON COLUMN STOCK_" + code + ".TURNOVER IS '换手率'",
#                "COMMENT ON COLUMN STOCK_" + code + ".MARKETCAP IS '总市值'",
#                "COMMENT ON COLUMN STOCK_" + code + ".FAMC IS '流通市值'",
#                "COMMENT ON COLUMN STOCK_" + code + ".ZBS IS '成交笔数'" ]
#    for i in comments:
#       cursor.execute(i)


# 判断表是否已存在
def is_table_exist(code,cursor):
    table = "stock_" + code
    sql = " select TABLE_NAME from INFORMATION_SCHEMA.TABLES where  TABLE_NAME='%s' " %table
    print(sql)
    rs = cursor.execute(sql)
    result = cursor.fetchall()
    #print(result)
    if len(result) > 0:
        return True
        ptint(table+"True")
    else:
        return False
        ptint(table+"false")


def main(key, url):
    getCSV(key, url)


if __name__ == '__main__':

#=======================================================
    time1 = time.time()
    dict = listStock(cursor)
    pool = Pool(processes = 24)  # 设定并发进程的数量
    for key in dict:
        pool.apply_async(main, (key, dict[key], ))

    pool.close()
    pool.join()
    #main("600719","xxxx")
    time2 = time.time()
    print((time2 - time1) / 60, u"分钟")
