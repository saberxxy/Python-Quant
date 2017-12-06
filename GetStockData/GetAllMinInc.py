# -*- coding=utf-8 -*-
# 获取分钟级历史记录——增量

import re
import time
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


# 获取增量分钟数据
def getMinDataInc(cons, codes, systemTime):
    print(codes)
    for i in codes:
        startTime = cursor.execute("select to_char(max(sdate+1), 'yyyy-MM-dd') from stock_"+i+"_MIN").fetchall()[0][0]
        # print(endTime)
        insertDataInc(cons, i, startTime, systemTime)


#插入数据
def insertDataInc(cons, i, startTime, endTime):
    # 分钟数据, 设置freq参数，分别为1min/5min/15min/30min/60min，D(日)/W(周)/M(月)/Q(季)/Y(年)
    df = ts.bar(i, conn=cons, freq='1min', ma=[5, 10, 20, 60],
                start_date=str(startTime), end_date=str(endTime))
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

    try:
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
        print('------插入增量数据成功------' + i)
    except Exception:
        pass

# 将科学记数法化为浮点数
def as_num(x):
    y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
    return(y)


def main():
    # 获取连接备用
    cons = ts.get_apis()
    codes = getCode()
    # print(codes)

    # 获取结束时间
    systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
    getMinDataInc(cons, codes, systemTime)

    # 关闭连接
    ts.close_apis(cons)


if __name__ == '__main__':
    main()
