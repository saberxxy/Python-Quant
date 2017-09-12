# -*- coding=utf-8 -*-
# 获取沪市融资融券明细数据

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
    cursor.execute("select count(1) from stock_sh_margin_details")
    pdata = cursor.fetchone()
    return pdata['count(1)']


def getShMarginDetails(cursor):
    for i in range(1992, 2017+1):
        try:
            df = ts.sh_margin_details(start=str(i)+'-01-01', end=str(i)+'-12-31', pause=0.01)

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

                cursor.execute("insert into stock_sh_margin_details(uuid, op_date, code, name, rzye, rzmre, "
                           "rzche, rqyl, rqmcl, rqchl) "
                           "values('%s','%s' ,'%s', '%s', '%.4f', '%.4f', "
                           "'%.4f', '%.4f', '%.4f', '%.4f')" % (str(list(df2['uuid'])[0]),
                            str(list(df2['opDate'])[0]),
                            str(list(df2['stockCode'])[0]),
                            str(list(df2['securityAbbr'])[0]),
                            round(float(df2['rzye']), 4),
                            round(float(df2['rzmre']), 4),
                            round(float(df2['rzche']), 4),
                            round(float(df2['rqyl']), 4),
                            round(float(df2['rqmcl']), 4),
                            round(float(df2['rqchl']), 4)) )
            cursor.execute("commit")
        except Exception as e :
            pass
            print(e)


def main():
    cursor = conn.getConfig()
    # print(cursor)
    pdata = haveData(cursor)
    if pdata == 0:
        getShMarginDetails(cursor)
    else:
        cursor.execute("truncate table stock_sh_margin_details")
        print("发现数据，清除完毕")
        getShMarginDetails(cursor)



if __name__ == '__main__':
    main()
