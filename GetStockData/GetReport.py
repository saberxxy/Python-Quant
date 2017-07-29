# -*- coding=utf-8 -*-
# 获取业绩报告

import tushare as ts
import cx_Oracle as cxo
import configparser

# 从1992年第二季度到2017年第二季度
# 返回数据格式
# code,代码
# name,名称
# eps,每股收益
# eps_yoy,每股收益同比(%)
# bvps,每股净资产
# roe,净资产收益率(%)
# epcf,每股现金流量(元)
# net_profits,净利润(万元)
# profits_yoy,净利润同比(%)
# distrib,分配方案
# report_date,发布日期

def getConfig():
    cf = configparser.ConfigParser()
    cf.read("config.conf")
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

# 检查表中是否存在数据
def haveData(cursor):
    cursor.execute("select count(1) from stock_report")
    pdata = cursor.fetchone()
    return pdata[0]

def getReport(cursor):
    for i in range(1992, 2017+1):
        for j in range(1, 4+1):
            try:
                print(i, j)
                df = ts.get_report_data(i, j)
                stockCode = list(df['code'])
                stockName = list(df['name'])
                stockEps = list(df['eps'])
                stockEps_yoy = list(df['eps_yoy'])
                stockBvps = list(df['bvps'])
                stockRoe = list(df['roe'])
                stockEpcf = list(df['epcf'])
                stockNet_profits = list(df['net_profits'])
                stockProfits_yoy = list(df['profits_yoy'])
                stockDistrib = list(df['distrib'])
                stockReport_date = list('2017-' + df['report_date'])

                dfLen = len(df)

                for k in range(0, dfLen):
                    stockCodeDB = stockCode[k]
                    stockNameDB = stockName[k]

                    if str(stockEps[k]) == 'nan':
                        stockEpsDB = 0.0
                    else:
                        stockEpsDB = stockEps[k]

                    if str(stockEps_yoy[k]) == 'nan':
                        stockEps_yoyDB = 0.0
                    else:
                        stockEps_yoyDB = stockEps_yoy[k]

                    if str(stockBvps[k]) == 'nan':
                        stockBvpsDB = 0.0
                    else:
                        stockBvpsDB = round(float(stockBvps[k]), 4)

                    if str(stockRoe[k]) == 'nan':
                        stockRoeDB = 0.0
                    else:
                        stockRoeDB = round(float(stockRoe[k]), 4)

                    if str(stockEpcf[k]) == 'nan':
                        stockEpcfDB = 0.0
                    else:
                        stockEpcfDB = round(float(stockEpcf[k]), 4)

                    if str(stockNet_profits[k]) == 'nan':
                        stockNet_profitsDB = 0.0
                    else:
                        stockNet_profitsDB = round(float(stockNet_profits[k]), 4)

                    if str(stockProfits_yoy[k]) == 'nan':
                        stockProfits_yoyDB = 0.0
                    else:
                        stockProfits_yoyDB = round(float(stockProfits_yoy[k]), 4)

                    stockDistribDB = stockDistrib[k]

                    stockReport_dateDB = str(stockReport_date[k])[0:4] + '-' + str(stockReport_date[k])[5:7] + '-' + \
                                         str(stockReport_date[k])[8:10]
                    stockYearDB = str(i)
                    stockQuarterDB = str(j)
                    cursor.execute("insert into stock_report(UUID, CODE, NAME, ESP, EPS_YOY, BVPS, ROE,"
                                   "EPCF, NET_PROFITS, PROFITS_YOY, DISTRIB, REPORT_DATE, YEAR, QUARTER)"
                                   "values(sys_guid(), '%s', '%s', '%f', '%f', '%f', '%f', "
                                   "'%f', '%f', '%f', '%s', to_date('%s', 'yyyy-MM-dd'), '%s', '%s')" % (
                                       stockCodeDB, stockNameDB, stockEpsDB, stockEps_yoyDB, stockBvpsDB, stockRoeDB,
                                       stockEpcfDB, stockNet_profitsDB, stockProfits_yoyDB, stockDistribDB,
                                       stockReport_dateDB, stockYearDB, stockQuarterDB))
                    cursor.execute("commit")
            except Exception:
                pass


def main():
    cursor = getConfig()
    pdata = haveData(cursor)
    if pdata == 0:
        getReport(cursor)
    else:
        cursor.execute("truncate table stock_report")
        print("发现数据，清除完毕")
        getReport(cursor)


if __name__ == '__main__':
    main()



# print(stockReport_date)
# print(a)

# 获取数据库连接
