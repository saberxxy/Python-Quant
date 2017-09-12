# -*- coding=utf-8 -*-
# 获取分配预案

import tushare as ts
import uuid
from sqlalchemy import create_engine
#import cx_Oracle as cxo
import configparser

# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_profit_data")
    pdata = cursor.fetchall()
    return pdata[0]['count(1)']


def getProfitData(cursor):
    for i in range(1992, 2017+1):
        try:
            df = ts.profit_data(top=1000, year=i)

            # 处理缺失值
            df = df.fillna(0)
            df = df.stack().replace('--', '').unstack()

            dfLen = len(df)
            uuidList = []  # 添加uuid
            yearList = []  # 添加年份
            for l in range(0, dfLen):
                uuidList.append(uuid.uuid1())
                yearList.append(str(i))

            df['uuid'] = uuidList
            df['year'] = yearList

            for k in range(0, dfLen):
                df2 = df[k:k+1]
                cursor.execute("insert into stock_profit_data(uuid, code, name, year, report_date, divi, shares) "
                           "values('%s', '%s', '%s', '%s','%s', '%d', '%d')" % (str(list(df2['uuid'])[0]), 
                               str(list(df2['code'])[0]), str(list(df2['name'])[0]), str(list(df2['year'])[0]),
                            str(list(df2['report_date'])[0]), round(float(df2['divi']), 4), round(float(df2['shares']), 4)) )
            cursor.execute("commit")
        except Exception as e:
            pass
            print(e)


def main():
    cursor = conn.getConfig()
    getProfitData(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getProfitData(cursor)
    else:
        cursor.execute("truncate table stock_profit_data")
        print("发现数据，清除完毕")
        getProfitData(cursor)


if __name__ == '__main__':
    main()
