# -*- coding=utf-8 -*-
# 获取分钟级历史记录

import tushare as ts
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


# 获取所有股票代码
def getCode():
    codes = []
    a = cursor.execute("select code from stock_basics")
    for i in a.fetchall():
        codes.append(i[0])
    return codes


# 获取分钟数据
def getMinData(cons, codes):
    print(codes)
    for i in codes:
        # 建表
        if not is_table_exist(i):  # 如果表不存在，先创建表
            create_table(i)  # 如果表不存在，先建表，再插入数据
            insertData(cons, i)
        else:
            # 存在则判断是否有数据
            # cursor.execute("truncate table stock_" + i + "_MIN")
            result = cursor.execute("select count(1) from stock_" + i + "_MIN")
            for x in result.fetchall():
                if x[0] < 24000:
                    insertData(cons, i)
                else:
                    print('----------pass----------'+i)
                    pass


# 将科学记数法化为浮点数
def as_num(x):
    y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
    return(y)

# 判断表是否已存在
def is_table_exist(code):
    table = "STOCK_" + code + "_MIN"
    sql = "select table_name from user_tables"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    return tables.__contains__(table)

# 建表
def create_table(code):
    sql = "CREATE TABLE STOCK_" + code + "_MIN" + """
    (
            UUID VARCHAR2(80) PRIMARY KEY,
            SDATE DATE,
            CODE VARCHAR2(20),
            OPEN NUMBER(20, 4),
            CLOSE NUMBER(20, 4),
            HIGH NUMBER(20, 4),
            LOW NUMBER(20, 4),
            VOL NUMBER(20, 4),
            AMOUNT NUMBER(20, 4),
            MA5 NUMBER(20, 4),
            MA10 NUMBER(20, 4),
            MA20 NUMBER(20, 4),
            MA60 NUMBER(20, 4)
        )
    """
    cursor.execute(sql)
    # 添加注释
    comments = ["COMMENT ON TABLE STOCK_" + code + "_MIN" + " IS '" + code + "'",  # 表注释
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".UUID IS 'UUID'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".SDATE IS '日期'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".CODE IS '股票代码'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".OPEN IS '开盘价'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".CLOSE IS '收盘价'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".HIGH IS '最高价'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".LOW IS '最低价'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".VOL IS '成交量'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".AMOUNT IS '成交金额'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".MA5 IS '5个周期平均'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".MA10 IS '10个周期平均'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".MA20 IS '20个周期平均'",
                "COMMENT ON COLUMN STOCK_" + code + "_MIN" + ".MA60 IS '60个周期平均'"]
    for i in comments:
        cursor.execute(i)

#插入数据
def insertData(cons, i):
    try:
        # 分钟数据, 设置freq参数，分别为1min/5min/15min/30min/60min，D(日)/W(周)/M(月)/Q(季)/Y(年)
        df = ts.bar(i, conn=cons, freq='1min', ma=[5, 10, 20, 60],
                    start_date='1990-01-01', end_date='')

        dfMin = pd.DataFrame()
        dfMin['code'] = df['code']
        dfMin['time'] = df.index
        # 转浮点数
        dfMin['open'] = [as_num(x) for x in df['open']]

        dfMin['close'] = df['close']
        dfMin['high'] = df['high']
        dfMin['low'] = df['low']
        dfMin['vol'] = df['vol']
        dfMin['amount'] = df['amount']
        dfMin['ma5'] = df['ma5']
        dfMin['ma10'] = df['ma10']
        dfMin['ma20'] = df['ma20']
        dfMin['ma60'] = df['ma60']

        dfLen = len(dfMin)
        dfMin['uuid'] = [uuid.uuid1() for l in range(0, dfLen)]  # 添加uuid

        # 处理None
        dfMin = dfMin.replace('None', 0)

        # print(df.head(5))
        # print(dfMin.head(5))

        for k in range(0, dfLen):
            df2 = dfMin[k:k + 1]
            sql = "insert into stock_" + str(
                i) + "_min (uuid, sdate, code, open, close, high, low, vol, amount, ma5, ma10, ma20, ma60) " \
                     "values(:uuid, to_date(:sdate, 'yyyy-MM-dd hh24:mi:ss'), :code, :open, :close, :high, :low, :vol, :amount, :ma5, :ma10, :ma20, :ma60)"
            cursor.execute(sql,
                           (str(list(df2['uuid'])[0]),
                            str(list(df2['time'])[0]),
                            str(list(df2['code'])[0]),
                            round(float(df2['open']), 4),
                            round(float(df2['close']), 4),
                            round(float(df2['high']), 4),
                            round(float(df2['low']), 4),
                            round(float(df2['vol']), 4),
                            round(float(df2['amount']), 4),
                            round(float(df2['ma5']), 4),
                            round(float(df2['ma10']), 4),
                            round(float(df2['ma20']), 4),
                            round(float(df2['ma60']), 4)
                            )
                           )
        cursor.execute("commit")
        print('------插入数据成功------' + i)
    except Exception:
        pass

def main():
    # 获取连接备用
    cons = ts.get_apis()
    codes = getCode()
    # print(codes)
    getMinData(cons, codes)

    # 关闭连接
    ts.close_apis(cons)


if __name__ == '__main__':
    main()
