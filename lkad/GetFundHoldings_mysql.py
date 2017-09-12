# -*- coding=utf-8 -*-
# 获取基金持股

import tushare as ts
import uuid
from sqlalchemy import create_engine
import configparser

# 导入连接文件
import sys
sys.path.append("..")
import common.GetMysqlConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_fund_holdings")
    pdata = cursor.fetchall()
    return pdata[0]['count(1)']


def getFundHoldings(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.fund_holdings(i, j)

                # 处理缺失值
                df = df.fillna(0)

                dfLen = len(df)
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

                    cursor.execute("insert into stock_fund_holdings(uuid, code, name, report_date, nums, nlast, "
                               "count, clast, amount, ratio, year, quarter) "
                               "values('%s','%s','%s', '%s',' %s','%s' , "
                               "'%s','%s', '%s', '%s', '%s', '%s')" %   (str(list(df2['uuid'])[0]),
                                str(list(df2['code'])[0]),
                                str(list(df2['name'])[0]),
                                str(list(df2['date'])[0]),
                                round(float(df2['nums']), 4),
                                round(float(df2['nlast']), 4),
                                round(float(df2['count']), 4),
                                round(float(df2['clast']), 4),
                                round(float(df2['amount']), 4),
                                round(float(df2['ratio']), 4),
                                str(list(df2['year'])[0]),
                                str(list(df2['quarter'])[0])) )
                cursor.execute("commit")
            except Exception as e:
                pass
                print(e)


def main():
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getFundHoldings(cursor)
    else:
        cursor.execute("truncate table stock_fund_holdings")
        print("发现数据，清除完毕")
        getFundHoldings(cursor)



if __name__ == '__main__':
    main()
