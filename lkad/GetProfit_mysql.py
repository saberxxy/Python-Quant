# -*- coding=utf-8 -*-
# 获取盈利能力

import tushare as ts
import uuid
from sqlalchemy import create_engine
import cx_Oracle as cxo
import configparser

# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_profit")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getProfit(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.get_profit_data(i, j)

                # 处理缺失值
                df = df.fillna(0)
                #print(df)

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

                    cursor.execute("insert into stock_profit(uuid, code, name, roe, net_profit_ratio, gross_profit_rate, "
                               "net_profits, eps, business_income, bips, year, quarter) "
                               "values('%s', '%s', '%s', '%.4f', '%.4f', '%.4f', "
                               "'%.4f', '%.4f', '%.4f', '%.4f', '%s', '%s')" % (str(list(df2['uuid'])[0]),
                                   str(list(df2['code'])[0]), str(list(df2['name'])[0]), round(float(df2['roe']), 4),
                                round(float(df2['net_profit_ratio']), 4), round(float(df2['gross_profit_rate']), 4),
                                round(float(df2['net_profits']), 4), round(float(df2['eps']), 4), round(float(df2['business_income']), 4),
                                round(float(df2['bips']), 4), str(list(df2['year'])[0]), str(list(df2['quarter'])[0])) )
                cursor.execute("commit")
            except Exception as e :
                pass
                print(e)


def main():
    cursor = conn.getConfig()
    pdata = haveData(cursor)
    if pdata == 0:
        getProfit(cursor)
    else:
        cursor.execute("truncate table stock_profit")
        print("发现数据，清除完毕")
        getProfit(cursor)



if __name__ == '__main__':
    main()
