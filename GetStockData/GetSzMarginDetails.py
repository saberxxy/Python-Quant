# -*- coding=utf-8 -*-
# 获取深市融资融券明细数据

import tushare as ts
import uuid
from sqlalchemy import create_engine
import cx_Oracle as cxo
import configparser
import datetime

# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_sz_margin_details")
    pdata = cursor.fetchone()
    return pdata[0]


def getSzMarginDetails(cursor):

    dateList = getDate('2010-05-17', '2017-12-31')
    print(len(dateList))

    for i in dateList:
        try:
            # 深市融资融券明细一次只能获取一天的明细数据，如果不输入参数，则为最近一个交易日的明细数据
            print(i)
            df = ts.sz_margin_details(str(i), pause=0.01)

            # 处理缺失值
            df = df.fillna(0)
            print(df)

            dfLen = len(df)
            uuidList = []  # 添加uuid
            for l in range(0, dfLen):
                uuidList.append(uuid.uuid1())
            df['uuid'] = uuidList

            for k in range(0, dfLen):
                df2 = df[k:k+1]

                cursor.execute("insert into stock_sz_margin_details(uuid, op_date, code, name, rzmre, rzye, "
                           "rqmcl, rqyl, rqye, rzrqye) "
                           "values(:uuid, to_date(:op_date, 'yyyy-MM-dd'), :code, :name, :rzmre, :rzye, "
                           ":rqmcl, :rqyl, :rqye, :rzrqye)",
                           (str(list(df2['uuid'])[0]),
                            str(list(df2['opDate'])[0]),
                            str(list(df2['stockCode'])[0]),
                            str(list(df2['securityAbbr'])[0]),
                            round(float(df2['rzmre']), 4),
                            round(float(df2['rzye']), 4),
                            round(float(df2['rqmcl']), 4),
                            round(float(df2['rqyl']), 4),
                            round(float(df2['rqye']), 4),
                            round(float(df2['rzrqye']), 4)) )
            cursor.execute("commit")
        except Exception:
            pass


# 获取特定日期中的每一天日期
def getDate(beginDate, endDate):
    dateList = []
    beginDate = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")
    while beginDate <= endDate:
        dateStr = beginDate.strftime("%Y-%m-%d")
        dateList.append(dateStr)
        beginDate += datetime.timedelta(days=1)
    return dateList

def main():
#======================================================================
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getSzMarginDetails(cursor)
    else:
        cursor.execute("truncate table stock_sz_margin_details")
        print("发现数据，清除完毕")
        getSzMarginDetails(cursor)






if __name__ == '__main__':
    main()