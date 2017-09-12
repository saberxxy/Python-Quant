# -*- coding=utf-8 -*-
# 获取现金流量

import tushare as ts
import uuid
from sqlalchemy import create_engine
#import cx_Oracle as cxo
import configparser
import pandas

# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_cashflow")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getCashflow(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.get_cashflow_data(i, j)

                # 处理缺失值
                df = df.fillna(0)
                # print(df)

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

                for k in range(0, dfLen):
                    df2 = df[k:k+1]

                    cursor.execute("insert into stock_cashflow(uuid, code, name, cf_sales, rateofreturn, cf_nm, "
                               "cf_liabilities, cashflowratio, year, quarter) "
                               "values('%s', '%s', '%s', '%.4f', '%.4f', '%.4f', "
                               "'%.4f', '%.4f', '%s', '%s')" % (str(list(df2['uuid'])[0]), 
                                   str(list(df2['code'])[0]), str(list(df2['name'])[0]), round(float(df2['cf_sales']), 4),
                                round(float(df2['rateofreturn']), 4), round(float(df2['cf_nm']), 4),
                                round(float(df2['cf_liabilities']), 4), round(float(df2['cashflowratio']), 4),
                                str(list(df2['year'])[0]), str(list(df2['quarter'])[0])) )
                cursor.execute("commit")
            except Exception as e :
                print(e)
                pass


def main():
    cursor = conn.getConfig()
    pdata = haveData(cursor)
    if pdata == 0:
        getCashflow(cursor)
    else:
        cursor.execute("truncate table stock_cashflow")
        print("发现数据，清除完毕")
        getCashflow(cursor)




if __name__ == '__main__':
    main()
