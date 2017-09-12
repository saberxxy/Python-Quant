# -*- coding=utf-8 -*-
# 获取限售股解禁

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
    cursor.execute("select count(1) from stock_xsg")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getXsg(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 12+1):
            try:
                print(i, j)
                df = ts.xsg_data(year=i, month=j)

                # 处理缺失值
                df = df.stack().replace('--', '0').unstack()
                #print(df)

                dfLen = len(df)
                # print(dfLen)
                uuidList = []  # 添加uuid
                yearList = []  # 添加年份
                monthList = []  # 添加月份
                for l in range(0, dfLen):
                    uuidList.append(uuid.uuid1())
                    yearList.append(str(i))
                    monthList.append(str(j))
                df['uuid'] = uuidList
                df['year'] = yearList
                df['month'] = monthList

                for k in range(0, dfLen):
                    df2 = df[k:k+1]

                    print(df2)

                    cursor.execute("insert into stock_xsg(uuid, code, name, lift_date, count, ratio, year, month) "
                               "values(:uuid, :code, :name, to_date(:lift_date, 'yyyy-MM-dd'), :count, :ratio, :year, :month)",
                               (str(list(df2['uuid'])[0]), str(list(df2['code'])[0]), str(list(df2['name'])[0]),
                                str(list(df2['date'])[0]), round(float(df2['count']), 4), round(float(df2['ratio']), 4),
                                str(list(df2['year'])[0]), str(list(df2['month'])[0])) )
                cursor.execute("commit")
            except Exception:
                pass


def main():
    cursor = conn.getConfig()
    pdata = haveData(cursor)
    if pdata == 0:
        getXsg(cursor)
    else:
        cursor.execute("truncate table stock_xsg")
        print("发现数据，清除完毕")
        getXsg(cursor)


if __name__ == '__main__':
    main()
