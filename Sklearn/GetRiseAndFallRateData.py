#-*- coding=utf-8 -*-
#清洗数据，主要进行涨跌幅的清洗，为第二日涨跌幅的预测做出准备，并入库

import tushare as ts
from sqlalchemy import create_engine
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time
import MySQLdb
import datetime

"""数据库连接相关"""
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = MySQLdb.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                       db=mysqlDatabaseName, port=mysqlPort, charset="utf8")
tableName = 'szzs_org'
tableNameRiseAndFallRate = 'szzs_rise_and_fall_rate'

def riseAndFallRateCln():
    """数据清洗"""
    #查询表的列数
    cur1 = conn.cursor()
    cur1.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='test' AND table_name='%s';"
                    % (tableName))
    result1 = cur1.fetchone()[0]

        #查询表的行数
    cur2 = conn.cursor()
    cur2.execute('SELECT COUNT(*) FROM %s;' % (tableName))
    result2 = cur2.fetchone()[0]

        #表查询语句
    cur3 = conn.cursor()
    cur3.execute('SELECT * FROM %s ORDER BY date DESC;' % (tableName))
    result3 = cur3.fetchmany(result2)

    """数据清洗、改造并入库"""
    cur4 = conn.cursor()
    cur4.execute("DROP TABLE IF EXISTS szzs_rise_and_fall_rate;")
    sqlCreate = """create table szzs_rise_and_fall_rate
                        (date date comment '交易日期',
                        open varchar(100) comment '开盘价',
                        high varchar(100) comment '最高价',
                        close varchar(100) comment '收盘价',
                        low varchar(100) comment '最低价',
                        volume varchar(100) comment '成交量',
                        amount varchar(100) comment '成交金额',
                        yst_close varchar(100) comment '前收盘',
                        rise_fall_rate varchar(100) comment '涨跌幅',
                        rise_fall_rate_next varchar(100) comment '明日涨跌幅'
                        )"""
    cur4.execute(sqlCreate)

    try:
        for i in range(0, result2-1):
            #定义每一列数据
            nowDate = str(result3[i]).split(',')[0][-4:]+'-'+str(result3[i]).split(',')[1][1:]+'-'+str(result3[i]).split(',')[2][1:]
            print nowDate
            open = str(result3[i]).split(',')[5]
            high = str(result3[i]).split(',')[6]
            close = str(result3[i]).split(',')[7]
            low = str(result3[i]).split(',')[8]
            volume = str(result3[i]).split(',')[9][1:-1]
            amount = str(result3[i]).split(',')[10][1:-2]
            ystClose = result3[i + 1][3]
            riseFallRate = ((result3[i][3]-result3[i+1][3])/result3[i+1][3]) * 100  # 当日涨跌幅
            riseFallRateNext = ((result3[i-1][3]-result3[i][3])/result3[i][3]) * 100  # 第二日涨跌幅

            if result3[i-1][3] == 99.98:  #处理最后一天的数据
                riseFallRateNext = 999


            cur4.execute("INSERT INTO %s(DATE, OPEN, HIGH, CLOSE, LOW, VOLUME, AMOUNT, YST_CLOSE, rise_fall_rate, rise_fall_rate_next) "
                                 "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"
                                 % (tableNameRiseAndFallRate, nowDate, open, high, close, low, volume, amount, ystClose, riseFallRate, riseFallRateNext))
        cur4.execute('commit;')

        print '数据清洗入库完毕！'
    except Exception:
        pass

#辗转相除
def gcd(b, a):
    b,a = a,b%a
    if a == 0:
        return b
    else:
        return gcd(b, a)

def main():
    # print gcd(10, 20)
    riseAndFallRateCln()

if __name__ == '__main__':
    main()






