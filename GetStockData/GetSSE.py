# -*- coding=utf-8 -*-
# 获取上证指数数据

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
import cProfile


# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()

# 通过网易财经获取全量数据的CSV文件
def getCSV(code, url):
    fordername = 'AllStockData\\'
    filename = str(code) + '.CSV'
    with request.urlopen(url) as web:
        # 为防止编码错误，使用二进制写文件模式
        with open(fordername+filename, 'wb') as outfile:
            outfile.write(web.read())

    saveInDB(code)
    # print(code)
    # print("一只股票入库完毕")
    # 删除CSV文件
    # os.remove(fordername+filename)

# 将获取的数据入库
def saveInDB(code):
    # 建表
    if not is_table_exist(code):  # 如果表不存在，先创建表
        create_table(code)  # 如果表不存在，先建表
    else:  # 存在则截断
        cursor.execute("truncate table stock_" + code)

    # 解析CSV文件并数据清洗
    fordername = 'AllStockData\\'
    filename = str(code)+'.CSV'
    df = pd.read_csv(fordername + filename, encoding='gbk')

    df.rename(columns={u'日期': 'sdate', u'股票代码': 'code', u'名称': 'name', u'收盘价': 'close', u'最高价': 'high',
                       u'最低价': 'low', u'开盘价': 'open', u'前收盘': 'y_close', u'涨跌额': 'p_change', u'涨跌幅': 'p_change_rate',
                       u'换手率': 'turnover', u'成交量': 'volume', u'成交金额': 'amount', u'总市值': 'marketcap',
                       u'流通市值': 'famc', u'成交笔数': 'zbs'}, inplace=True)
    df['code'] = code
    df['classify'] = "指数"
    dfLen = len(df)
    df['uuid'] = [uuid.uuid1() for l in range(0, dfLen)]  # 添加uuid

    # 处理None
    df = df.replace('None', 0)

    # 转浮点数
    df['amount'] = [float(x) for x in df['amount']]
    df['marketcap'] = 0
    df['famc'] = 0
    df['turnover'] = 0

    # 入库
    for k in range(0, dfLen):
        df2 = df[k:k + 1]
        sql = "insert into stock_"+str(code)+"(uuid, sdate, code, name, classify, open, close, high, low," \
          "volume, amount, y_close, p_change, p_change_rate, turnover, marketcap, famc, zbs) " \
          "values(:uuid, to_date(:sdate, 'yyyy-MM-dd'), :code, :name, :classify, :open, :close, :high, :low, " \
          ":volume, :amount, :y_close, :p_change, :p_change_rate, :turnover, :marketcap, :famc, :zbs)"

        cursor.execute(  sql,
                     ( str(list(df2['uuid'])[0]),
                       str(list(df2['sdate'])[0]),
                       str(list(df2['code'])[0]),
                       str(list(df2['name'])[0]),
                       str(list(df2['classify'])[0]),
                       round(float(df2['open']), 4),
                       round(float(df2['close']), 4),
                       round(float(df2['high']), 4),
                       round(float(df2['low']), 4),
                       round(float(df2['volume']), 4),
                       round(float(df2['amount']), 4),
                       round(float(df2['y_close']), 4),
                       round(float(df2['p_change']), 4),
                       round(float(df2['p_change_rate']), 4),
                       round(float(df2['turnover']), 4),
                       round(float(df2['marketcap']), 4),
                       round(float(df2['famc']), 4),
                       round(float(df2['zbs']), 4)
                     )
                  )
    cursor.execute("commit")

# 将科学记数法化为浮点数
def as_num(x):
    y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
    return(y)

# 建表
def create_table(code):
    sql = "CREATE TABLE STOCK_" + code + """
    (
            UUID VARCHAR2(80) PRIMARY KEY,
            SDATE DATE,
            CODE VARCHAR2(20),
            NAME VARCHAR2(80),
            CLASSIFY VARCHAR(80),
            OPEN NUMBER(20, 4),
            CLOSE NUMBER(20, 4),
            HIGH NUMBER(20, 4),
            LOW NUMBER(20, 4),
            VOLUME NUMBER(20, 4),
            AMOUNT NUMBER(20, 4),
            Y_CLOSE NUMBER(20, 4),
            P_CHANGE NUMBER(20, 4),
            P_CHANGE_RATE NUMBER(20, 4),
            TURNOVER NUMBER(20, 4),
            MARKETCAP NUMBER(30, 4),
            FAMC NUMBER(30, 4),
            ZBS NUMBER(20, 4)
        )
    """
    cursor.execute(sql)
    # 添加注释
    comments = ["COMMENT ON TABLE STOCK_" + code + " IS '" + code + "'",  # 表注释
                "COMMENT ON COLUMN STOCK_" + code + ".UUID IS 'UUID'",
                "COMMENT ON COLUMN STOCK_" + code + ".SDATE IS '日期'",
                "COMMENT ON COLUMN STOCK_" + code + ".CODE IS '股票代码'",
                "COMMENT ON COLUMN STOCK_" + code + ".NAME IS '股票名称'",
                "COMMENT ON COLUMN STOCK_" + code + ".CLASSIFY IS '类别'",
                "COMMENT ON COLUMN STOCK_" + code + ".OPEN IS '开盘价'",
                "COMMENT ON COLUMN STOCK_" + code + ".CLOSE IS '收盘价'",
                "COMMENT ON COLUMN STOCK_" + code + ".HIGH IS '最高价'",
                "COMMENT ON COLUMN STOCK_" + code + ".LOW IS '最低价'",
                "COMMENT ON COLUMN STOCK_" + code + ".VOLUME IS '成交量'",
                "COMMENT ON COLUMN STOCK_" + code + ".AMOUNT IS '成交金额'",
                "COMMENT ON COLUMN STOCK_" + code + ".Y_CLOSE IS '昨收盘'",
                "COMMENT ON COLUMN STOCK_" + code + ".P_CHANGE IS '涨跌额'",
                "COMMENT ON COLUMN STOCK_" + code + ".P_CHANGE_RATE IS '涨跌幅'",
                "COMMENT ON COLUMN STOCK_" + code + ".TURNOVER IS '换手率'",
                "COMMENT ON COLUMN STOCK_" + code + ".MARKETCAP IS '总市值'",
                "COMMENT ON COLUMN STOCK_" + code + ".FAMC IS '流通市值'",
                "COMMENT ON COLUMN STOCK_" + code + ".ZBS IS '成交笔数'" ]
    for i in comments:
        cursor.execute(i)

# 判断表是否已存在
def is_table_exist(code):
    table = "STOCK_" + code
    sql = "select table_name from user_tables"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    print(table)
    print(tables.__contains__(table))
    return tables.__contains__(table)

def main():
    systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
    code = "000001_SSE"
    url = "http://quotes.money.163.com/service/chddata.html?code=0000001&start=19901219&end="+systemTime
    getCSV(code, url)

if __name__ == '__main__':
    main()