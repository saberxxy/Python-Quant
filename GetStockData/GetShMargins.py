# -*- coding=utf-8 -*-
# 获取沪市融资融券汇总数据

"""
沪市的融资融券数据从上海证券交易所网站直接获取，提供了有记录以来的全部汇总和明细数据。根据上交所网站提示：数据根据券商申报的数据汇总，由券商保证数据的真实、完整、准确。

本日融资融券余额＝本日融资余额＋本日融券余量金额
本日融资余额＝前日融资余额＋本日融资买入额－本日融资偿还额；
本日融资偿还额＝本日直接还款额＋本日卖券还款额＋本日融资强制平仓额＋本日融资正权益调整－本日融资负权益调整；
本日融券余量=前日融券余量+本日融券卖出数量-本日融券偿还量；
本日融券偿还量＝本日买券还券量＋本日直接还券量＋本日融券强制平仓量＋本日融券正权益调整－本日融券负权益调整－本日余券应划转量；
融券单位：股（标的证券为股票）/份（标的证券为基金）/手（标的证券为债券）。
明细信息中仅包含当前融资融券标的证券的相关数据，汇总信息中包含被调出标的证券范围的证券的余额余量相关数据。

"""
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
    cursor.execute("select count(1) from stock_sh_margins")
    pdata = cursor.fetchone()
    return pdata[0]


def getShMargins(cursor):
    # try:
    startTime = '1990-01-01'
    endTime = '2020-01-01'
    # df = ts.sh_margins(start=startTime, end=endTime)
    df = ts.sh_margins(start=startTime, end=endTime)

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

        cursor.execute("insert into stock_sh_margins(uuid, op_date, rzye, rzmre, "
                   "rqyl, rqylje, rqmcl, rzrqjyzl ) "
                   "values(:uuid, to_date(:op_date, 'yyyy-MM-dd'), :rzye, :rzmre, "
                   ":rqyl, :rqylje, :rqmcl, :rzrqjyzl)" ,
                   (str(list(df2['uuid'])[0]),
                    str(list(df2['opDate'])[0]),
                    round(float(df2['rzye']), 4),
                    round(float(df2['rzmre']), 4),
                    round(float(df2['rqyl']), 4),
                    round(float(df2['rqylje']), 4),
                    round(float(df2['rqmcl']), 4),
                    round(float(df2['rzrqjyzl']), 4)) )
    cursor.execute("commit")
    # except Exception:
    #     pass


def main():
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getShMargins(cursor)
    else:
        cursor.execute("truncate table stock_sh_margins")
        print("发现数据，清除完毕")
        getShMargins(cursor)



if __name__ == '__main__':
    main()