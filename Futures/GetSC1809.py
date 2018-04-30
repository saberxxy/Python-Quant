# 获取SC1809数据

import urllib
import urllib.request
from urllib import request
from bs4 import BeautifulSoup
import demjson
import re
import pandas as pd
import uuid
import time

# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn

# 获取网页数据
def getSoup(url):
    content = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(content, "html.parser")
    a = demjson.encode(str(soup))
    b = demjson.decode(a)
    # print(b)
    result = str(re.findall(".*\[\[(.*)\]\].*", b)).replace("'", "")
    return result

# 存入数据库
def saveDB(result):
    # 时间，现价，成交额，成交量，持仓量
    # DateTime，Price，Amount，Volume，OpenInterest
    df = pd.DataFrame()
    date_list = []
    price_list = []
    amount_list= []
    volume_list = []
    opin_list = []
    for i in result.split("]"):
        result2 = i.replace(", [", "").replace("[", "").replace(" ", "").split(",")
        s = result2[0]
        if s != '':
            s1 = s[0:4] + "-" + s[4:6] + "-" + s[6:8] + ' ' + s[8:10] + ':' + s[10:12] + ':' + s[12:14]
            date_list.append(s1)
            price_list.append(float(result2[1])/10)
            amount_list.append(float(result2[2])/10)
            volume_list.append(float(result2[3]))
            opin_list.append(float(result2[5]))
    # print(volume_list)
    df['sdate'] = date_list
    df['price'] = price_list
    df['amount'] = amount_list
    df['volume'] = volume_list
    df['opin'] = opin_list
    df['uuid'] = [uuid.uuid1() for l in range(0, len(date_list))]
    df['code'] = code

    print(len(df))

    # 入库
    for k in range(0, len(df)):
        df2 = df[k:k + 1]
        sql = "insert into FUTURES_" + str(code) + \
              "(uuid, sdate, code, price, amount, volume, opin) " \
              "values(:uuid, to_date(:sdate, 'yyyy-MM-dd hh24:mi:ss'), :code, " \
              ":price, :amount, :volume, :opin)"
        cursor.execute(sql,
                       (str(list(df2['uuid'])[0]),
                        str(list(df2['sdate'])[0]),
                        str(list(df2['code'])[0]),
                        round(float(df2['price']), 4),
                        round(float(df2['amount']), 4),
                        round(float(df2['volume']), 4),
                        round(float(df2['opin']), 4)
                        )
                       )
    cursor.execute("commit")

    print('存储完毕')

# 判断表是否已存在
def is_table_exist(code):
    table = "FUTURES_" + code
    sql = "select table_name from user_tables"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    print(table)
    print(tables.__contains__(table))
    return tables.__contains__(table)

# 建表
def create_table(code):
    # 时间，现价，成交额，成交量，持仓量
    # DateTime，Price，Amount，Volume，OpenInterest
    sql = "CREATE TABLE FUTURES_" + code + """
    (
            UUID VARCHAR2(80) PRIMARY KEY,
            SDATE DATE,
            CODE VARCHAR2(20),
            PRICE NUMBER(30,4),
            AMOUNT NUMBER(30,4),
            VOLUME NUMBER(30,4),
            OPIN NUMBER(30,4)
        )
    """
    cursor.execute(sql)
    # 添加注释
    comments = ["COMMENT ON TABLE FUTURES_" + code + " IS '" + code + "'",
                "COMMENT ON COLUMN FUTURES_" + code + ".UUID IS 'UUID'",
                "COMMENT ON COLUMN FUTURES_" + code + ".SDATE IS '时间'",
                "COMMENT ON COLUMN FUTURES_" + code + ".CODE IS '期货代码'",
                "COMMENT ON COLUMN FUTURES_" + code + ".PRICE IS '现价'",
                "COMMENT ON COLUMN FUTURES_" + code + ".AMOUNT IS '成交额'",
                "COMMENT ON COLUMN FUTURES_" + code + ".VOLUME IS '成交量'",
                "COMMENT ON COLUMN FUTURES_" + code + ".OPIN IS '持仓量'" ]
    for i in comments:
        cursor.execute(i)


if __name__ == '__main__':
    # 获取全局数据库连接
    cursor = conn.getConfig()
    # print(cursor)
    # 由于数据只能按天获取，首先因此首先要解决的问题就是找出所有的交易日
    systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
    # print(systemTime)
    url_jy = "http://webftcn.hermes.hexun.com/shf/kline?code=SHFE2SC1809&start="+ \
             systemTime+"210000&number=-1000&type=5"
    result = getSoup(url_jy)
    jyr_list = []
    for i in result.split("]"):
        result2 = i.replace(", [", "").replace("[", "").replace(" ", "").split(",")
        # print(len(result2))
        if len(result2) > 1:
            if result2[0] != '':
                s = result2[0]
                s1 = s[0:8]
            elif result2[1] != '':
                s_1 = result2[1]
                s1 = s_1[0:8]
        jyr_list.append(s1)
    print(jyr_list)

    for my_date in jyr_list:
        code = "SHFE2SC1809"
        url = "http://webftcn.hermes.hexun.com/shf/historyminute?code="+code+"&date="+my_date
        result = getSoup(url)
        # 通过code判断数据库中是否已存在此表
        if not is_table_exist(code):  # 如果表不存在，先创建表
            create_table(code)  # 如果表不存在，先建表
        saveDB(result)




