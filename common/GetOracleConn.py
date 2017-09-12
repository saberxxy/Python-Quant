# -*- coding=utf-8 -*-
# 获取Oralce连接

import cx_Oracle as cxo
import configparser

def getConfig():
    cf = configparser.ConfigParser()
    cf.read("..\\config\\config.conf")
    oracleHost = str(cf.get("oracle", "ip"))
    oraclePort = int(cf.get("oracle", "port"))
    oracleUser = str(cf.get("oracle", "username"))
    oraclePassword = str(cf.get("oracle", "password"))
    oracleDatabaseName = str(cf.get("oracle", "databasename"))
    oracleConn = oracleUser + '/' + oraclePassword + '@' + oracleHost + '/' + oracleDatabaseName
    conn = cxo.connect(oracleConn)
    cursor = conn.cursor()
    print("已获取数据库连接")
    return cursor

if __name__ == '__main__':
    main()
