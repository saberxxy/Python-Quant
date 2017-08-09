# -*- coding=utf-8 -*-
# 获取运营能力

import tushare as ts
import uuid
from sqlalchemy import create_engine
import cx_Oracle as cxo
import configparser

# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn


# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_operation")
    pdata = cursor.fetchone()
    return pdata[0]


def getOperation(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.get_operation_data(i, j)

                # 处理缺失值
                df = df.fillna(0)
                print(df)

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

                    cursor.execute("insert into stock_operation(uuid, code, name, arturnover, arturndays, inventory_turnover, "
                               "inventory_days, currentasset_turnover, currentasset_days, year, quarter) "
                               "values(:uuid, :code, :name, :arturnover, :arturndays, :inventory_turnover, "
                               ":inventory_days, :currentasset_turnover, :currentasset_days, :year, :quarter)",
                               (str(list(df2['uuid'])[0]), str(list(df2['code'])[0]), str(list(df2['name'])[0]), round(float(df2['arturnover']), 4),
                                round(float(df2['arturndays']), 4), round(float(df2['inventory_turnover']), 4),
                                round(float(df2['inventory_days']), 4), round(float(df2['currentasset_turnover']), 4), round(float(df2['currentasset_days']), 4),
                                str(list(df2['year'])[0]), str(list(df2['quarter'])[0])) )
                cursor.execute("commit")
            except Exception:
                pass


def main():
    cursor = conn.getConfig()
    pdata = haveData(cursor)
    if pdata == 0:
        getOperation(cursor)
    else:
        cursor.execute("truncate table stock_operation")
        print("发现数据，清除完毕")
        getOperation(cursor)



if __name__ == '__main__':
    main()