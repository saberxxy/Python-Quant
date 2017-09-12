# -*- coding=utf-8 -*-
# 获取业绩预告

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
    cursor.execute("select count(1) from stock_forecast")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getForecast(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.forecast_data(i, j)

                # 处理缺失值
                df = df.stack().replace('--', '0').unstack()
                #print(df.ix[129])
                #print(df)
                #print(df.ix[131])
                ##处理'None'
               # df=df.replace('None',0)

                dfLen = len(df)
                # print(dfLen)
                uuidList = []  # 添加uuid
                yearList = []  # 添加年份
                quarterList = []  # 添加季度
                for l in range(0, dfLen):
                    uuidList.append(uuid.uuid1())
                    yearList.append(str(i))
                    quarterList.append(str(j))
                df['uuid'] = uuidList
                df['year'] = yearList
                df['quarter'] = quarterList
                #print("xxxxx")
                for k in range(0, dfLen):
                    df2 = df[k:k+1]

                    sql=("insert into stock_forecast(uuid, code, name, type, report_date, pre_eps, `range`, year, quarter) "
                               "value ('%s', '%s', '%s', '%s', '%s','%.4f', '%s','%s', '%s')"  % (str(list(df2['uuid'])[0]),
                                   str(list(df2['code'])[0]), str(list(df2['name'])[0]), str(list(df2['type'])[0]),
                                str(list(df2['report_date'])[0]), round(float(df2['pre_eps']), 4), str(list(df2['range'])[0]),
                                str(list(df2['year'])[0]), str(list(df2['quarter'])[0])) )
                    print(sql)
                    cursor.execute(sql)
                cursor.execute("commit")
            except Exception as e :
                print(e)
                pass


def main():
    cursor = conn.getConfig()
    #getForecast(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getForecast(cursor)
    else:
        cursor.execute("truncate table stock_forecast")
        print("发现数据，清除完毕")
        getForecast(cursor)


if __name__ == '__main__':
    main()
