# -*- coding=utf-8 -*-
# 获取深市融资融券汇总数据

"""
深市的融资融券数据从深圳证券交易所网站直接获取，提供了有记录以来的全部汇总和明细数据。在深交所的网站上，对于融资融券的说明如下：
说明：
本报表基于证券公司报送的融资融券余额数据汇总生成，其中：
本日融资余额(元)=前日融资余额＋本日融资买入-本日融资偿还额
本日融券余量(股)=前日融券余量＋本日融券卖出量-本日融券买入量-本日现券偿还量
本日融券余额(元)=本日融券余量×本日收盘价
本日融资融券余额(元)=本日融资余额＋本日融券余额；
2014年9月22日起，“融资融券交易总量”数据包含调出标的证券名单的证券的融资融券余额。
"""
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
    cursor.execute("select count(1) from stock_sz_margins")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getSzMargins(cursor):
    # try:
    for i in range(2010, 2017 + 1):
        df = ts.sz_margins(start=str(i)+'-01-01', end=str(i)+'-12-31', pause=0.01)

        # 处理缺失值
        df = df.fillna(0)
        #print(df)

        dfLen = len(df)
        uuidList = []  # 添加uuid
        for l in range(0, dfLen):
            uuidList.append(uuid.uuid1())
        df['uuid'] = uuidList

        for k in range(0, dfLen):
            df2 = df[k:k+1]

            cursor.execute("insert into stock_sz_margins(uuid, op_date, rzmre, rzye, "
                       "rqmcl, rqyl, rqye, rzrqye ) "
                       "values(:uuid, to_date(:op_date, 'yyyy-MM-dd'), :rzmre, :rzye, "
                       ":rqmcl, :rqyl, :rqye, :rzrqye)" ,
                       (str(list(df2['uuid'])[0]),
                        str(list(df2['opDate'])[0]),
                        round(float(df2['rzmre']), 4),
                        round(float(df2['rzye']), 4),
                        round(float(df2['rqmcl']), 4),
                        round(float(df2['rqyl']), 4),
                        round(float(df2['rqye']), 4),
                        round(float(df2['rzrqye']), 4) ) )
        cursor.execute("commit")
    # except Exception:
    #     pass


def main():
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getSzMargins(cursor)
    else:
        cursor.execute("truncate table stock_sz_margins")
        print("发现数据，清除完毕")
        getSzMargins(cursor)



if __name__ == '__main__':
    main()
