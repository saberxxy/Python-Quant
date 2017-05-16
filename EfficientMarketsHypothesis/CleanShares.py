#-*- coding=utf-8 -*-
#清洗股票信息，替换空值

import pymysql
import re

"""数据库连接相关"""
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                       db=mysqlDatabaseName, port=mysqlPort, charset="utf8")


def clean():
    cur1 = conn.cursor()
    cur1.execute("show tables")
    sharesList = []
    for i in cur1.fetchall():
        sharesList.append(i[0])

    for j in sharesList:
        pattern = re.match(r'^\d+_.*?_org$', j)
        if pattern != None:
            print(j)
            cur1.execute("select b_open, b_high, b_close, b_low, b_change from %s" % (j))
            for k in cur1.fetchall():
                if k[0] == None:
                    cur1.execute("update %s set b_open=0" % (j))
                elif k[1] == None:
                    cur1.execute("update %s set b_high=0" % (j))
                elif k[2] == None:
                    cur1.execute("update %s set b_close=0" % (j))
                elif k[3] == None:
                    cur1.execute("update %s set b_low=0" % (j))
                elif k[4] == None:
                    cur1.execute("update %s set b_change=0" % (j))
                else:
                    print('not null')

    print ('数据清洗完毕！')

def main():
    clean()
    pass


if __name__ == '__main__':
    main()