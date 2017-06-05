#-*- coding=utf-8 -*-
#获取证券数据

import tushare as ts
import sqlalchemy
import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")
import time
import pymysql
import datetime
import pandas as pd

"""数据库连接相关"""
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                       db=mysqlDatabaseName, port=mysqlPort, charset="utf8")
tableName = 'szzs_org'

def getData():
    """获取当前日期"""
    theTime = time.strftime("%Y-%m-%d", time.localtime())  #theTime当前日期
    # theTime = '2017-05-12'


    """获取数据并入库"""
    #判断表是否已存在
    curTableIsExsists = conn.cursor()
    curTableIsExsists.execute("SHOW TABLES LIKE '%s';" % (tableName))
    resultTableIsExsists = curTableIsExsists.fetchmany(1)
    if not resultTableIsExsists:  #表不存在则建立新表
        print ('Does not exists')
        df = ts.get_h_data('000001', start='1990-12-19', end=theTime, index=True)  # index表示是否为大盘指数，默认为False
    else:  #存在则获取时间后追加
        print ('Exists')
        # 获取数据库中所存数据的最近一日日期
        curTime = conn.cursor()
        curTime.execute('SELECT date FROM %s ORDER BY date DESC;' % (tableName))
        resultTime = curTime.fetchmany(1)
        lastTime = str(resultTime[0]).split(',')[0][-4:] + '-' + str(resultTime[0]).split(',')[1][1:] \
                   + '-' + str(resultTime[0]).split(',')[2][1:]  # lastTime所存数据的最近一日日期
        lastTime = datetime.datetime.strptime(lastTime, '%Y-%m-%d')  # 再次格式化日期，为日期比较加减准备
        lastTimeNext = str(lastTime + datetime.timedelta(days=1))
        lastTimeNext = lastTimeNext[0:-9]  # 获取最后一天的日期的后一天日期，并截取
        print (lastTimeNext)
        df = ts.get_h_data('000001', start=lastTimeNext, end=theTime, index=True) #index表示是否为大盘指数，默认为False


    engine = sqlalchemy.create_engine('mysql://root:root@127.0.0.1:3306/test')

    try:
        try:
            df.to_sql('szzs_org', engine, dtype=None, if_exists='append')
        except Exception:
            df.to_sql('szzs_org', engine, dtype=None, if_exists='append')
    except Exception:
        pass

    cur1 = conn.cursor()  #完成插入操作后提交
    cur1.execute("commit;")
    print (u'数据获取完成，插入已提交！')


def main():
    getData()

if __name__ == '__main__':
    main()