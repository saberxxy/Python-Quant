#-*- coding=utf-8 -*-
#清洗数据，主要进行黑天鹅的判断，并入库

import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")
import time
import pymysql
# import MySQLdb
import datetime

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
tableNameRiseAndFall = 'szzs_black_swan'

def blackSwanCln():
    """数据清洗"""
    try:
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
        cur4.execute("DROP TABLE IF EXISTS szzs_black_swan;")
        sqlCreate = """create table szzs_black_swan
                       (date date comment '交易日期',
                       open varchar(100) comment '开盘价',
                       high varchar(100) comment '最高价',
                       close varchar(100) comment '收盘价',
                       low varchar(100) comment '最低价',
                       volume varchar(100) comment '成交量',
                       amount varchar(100) comment '成交金额',
                       yst_close varchar(100) comment '前收盘',
                       rise_fall varchar(100) comment '涨跌',
                       rise_fall_next varchar(100) comment '明日涨跌',
                       rise_fall_rate varchar(100) comment '涨跌幅',
                       rise_fall_rate_next varchar(100) comment '明日涨跌幅',
                       black_swan varchar(100) comment '是否是黑天鹅，0白天鹅，1灰天鹅，2黑天鹅',
                       black_swan_next varchar(100) comment '明日是否是黑天鹅，0白天鹅，1灰天鹅，2黑天鹅'
                       )"""
        cur4.execute(sqlCreate)

        a,b,c,d = 0,0,0,0

        for i in range(0, result2-1):
            riseOrFall = result3[i][3]-result3[i+1][3]
            riseOrFallNext = result3[i-1][3]-result3[i][3]
            riseOrFallRate = ((result3[i][3]-result3[i+1][3])/result3[i+1][3])*100
            riseOrFallRateNext = ((result3[i-1][3]-result3[i][3])/result3[i][3])*100

            if  riseOrFall < 0:  #判断涨跌情况
                a = 0
            else:
                a = 1

            if riseOrFallNext < 0:  #判断第二天的涨跌情况
                b = 0
            else:
                b = 1

            # 判断是否为黑天鹅
            if riseOrFallRate>=-2 and riseOrFallRate*100<2:
                c = 0  #白天鹅
            elif (riseOrFallRate>=-5 and riseOrFallRate<-2) or (riseOrFallRate>=2 and riseOrFallRate<5):
                c = 1  #灰天鹅
            elif riseOrFallRate<-5 or riseOrFallRate>=5:
                c = 2  #黑天鹅

            if riseOrFallRateNext>=-2 and riseOrFallRateNext<2:
                d = 0  #白天鹅
            elif (riseOrFallRateNext>=-5 and riseOrFallRateNext<-2) or (riseOrFallRateNext>=2 and riseOrFallRateNext<5):
                d = 1  #灰天鹅
            elif riseOrFallRateNext<-5 or riseOrFallRateNext>=5:
                d = 2  #黑天鹅

            #定义每一列数据
            nowDate = str(result3[i]).split(',')[0][-4:]+'-'+str(result3[i]).split(',')[1][1:]+'-'+str(result3[i]).split(',')[2][1:]
            print (nowDate)
            open = str(result3[i]).split(',')[5]
            high = str(result3[i]).split(',')[6]
            close = str(result3[i]).split(',')[7]
            low = str(result3[i]).split(',')[8]
            volume = str(result3[i]).split(',')[9][1:-1]
            amount = str(result3[i]).split(',')[10][1:-2]
            ystClose = result3[i + 1][3]
            riseFall = int(a)
            riseFallNext = int(b)
            riseFallRate = riseOrFallRate  # 当日涨跌幅
            riseFallRateNext = riseOrFallRateNext  # 第二日涨跌幅
            blackSwan = int(c)
            blackSwanNext = int(d)

            # 处理最后一天的数据
            if result3[i - 1][3] == 99.98:
                riseFallNext = 999

            if result3[i - 1][3] == 99.98:
                riseFallRateNext = 999

            if result3[i - 1][3] == 99.98:
                blackSwanNext = 999

            cur4.execute("INSERT INTO %s(DATE, OPEN, HIGH, CLOSE, LOW, VOLUME, AMOUNT, YST_CLOSE, RISE_FALL, RISE_FALL_NEXT, RISE_FALL_RATE, RISE_FALL_RATE_NEXT, BLACK_SWAN, BLACK_SWAN_NEXT) "
                             "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%s', '%s', '%d', '%d');"
                             % (tableNameRiseAndFall, nowDate, open, high, close, low, volume, amount, ystClose, riseFall, riseFallNext, riseFallRate, riseFallRateNext, blackSwan, blackSwanNext))
        cur4.execute('commit;')
    except Exception:
        pass

    print (u'数据清洗入库完毕！')


def main():
    blackSwanCln()
    pass

if __name__ == '__main__':
    main()