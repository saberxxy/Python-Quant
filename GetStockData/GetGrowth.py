# -*- coding=utf-8 -*-
# 获取成长能力

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
    cursor.execute("select count(1) from stock_growth")
    pdata = cursor.fetchone()
    return pdata[0]


def getGrowth(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.get_growth_data(i, j)

                # 处理缺失值
                df = df.fillna(0)

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

                    cursor.execute("insert into stock_growth(uuid, code, name, mbrg, nprg, nav, "
                               "targ, epsg, seg, year, quarter) "
                               "values(:uuid, :code, :name, :mbrg, :nprg, :nav, "
                               ":targ, :epsg, :seg,  :year, :quarter)",
                               (str(list(df2['uuid'])[0]), str(list(df2['code'])[0]), str(list(df2['name'])[0]), round(float(df2['mbrg']), 4),
                                round(float(df2['nprg']), 4), round(float(df2['nav']), 4),
                                round(float(df2['targ']), 4), round(float(df2['epsg']), 4), round(float(df2['seg']), 4),
                                str(list(df2['year'])[0]), str(list(df2['quarter'])[0])) )
                cursor.execute("commit")
            except Exception:
                pass


def main():
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getGrowth(cursor)
    else:
        cursor.execute("truncate table stock_growth")
        print("发现数据，清除完毕")
        getGrowth(cursor)



if __name__ == '__main__':
    main()