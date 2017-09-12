# -*- coding=utf-8 -*-
# 获取Oralce连接

import pymysql 
import configparser
import os
def getConfig():
    cf = configparser.ConfigParser()
    cf.read(".."+os.path.sep+"config"+os.path.sep+"config.conf")
    mysqlHost = str(cf.get("mysql", "ip"))
    mysqlPort = int(cf.get("mysql", "port"))
    mysqlUser = str(cf.get("mysql", "username"))
    mysqlPassword = str(cf.get("mysql", "password"))
    mysqlDatabaseName = str(cf.get("mysql", "databasename"))
    mysqlconfig = {'host':mysqlHost,
                   'port':mysqlPort,
                   'user':mysqlUser,
                   'password':mysqlPassword,
                   'db':mysqlDatabaseName,
                   'charset':'utf8mb4',
                   'cursorclass':pymysql.cursors.DictCursor,
                   }
    conn = pymysql.connect(**mysqlconfig)
    #               'autocommit':"1"
    cursor = conn.cursor()
    print("已获取数据库连接")
    return cursor

if __name__ == '__main__':
    getConfig()
