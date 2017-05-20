#-*- coding=utf-8 -*-

import tushare as ts
import cx_Oracle as cxo
import configparser


# 获取数据
def getBasics(cursor):
    df = ts.get_stock_basics()

    stockCode = list(df.index)  #股票代码
    stockName = list(df['name'])  #股票名称
    stockIndustry = list(df['industry'])  #所属行业
    stockArea = list(df['area'])  #所在区域
    stockPe = list(df['pe'])  #市盈率
    stockOutstanding = list(df['outstanding'])  #流通股本(亿)
    stockTotals = list(df['totals'])  #总股本(亿)
    stockTotalAssets = list(df['totalAssets'])  #总资产(万)
    stockLiquidAssets = list(df['liquidAssets'])  #流动资产
    stockFixedAssets = list(df['fixedAssets'])  #固定资产
    stockReserved = list(df['reserved'])  #公积金
    stockReservedPerShare = list(df['reservedPerShare'])  #每股公积金
    stockEsp = list(df['esp'])  #每股收益
    stockBvps = list(df['bvps'])  #每股净资
    stockPb = list(df['pb'])  #市净率
    stockTimeToMarket = list(df['timeToMarket'])  #上市日期
    stockUndp = list(df['undp'])  #未分利润
    stockPerundp = list(df['perundp'])  #每股未分配
    stockRev = list(df['rev'])  #收入同比(%)
    stockProfit = list(df['profit'])  #利润同比(%)
    stockGpr = list(df['gpr'])  #毛利率(%)
    stockNpr = list(df['npr'])  #净利润率(%)
    stockHolders = list(df['holders'])  #股东人数
    print("已获取数据")
    # str1 = str(stockTimeToMarket[0])
    # print(str1[0:4])

    dfLen = len(df)

    #print(time.strptime(stockTimeToMarket[1], "%Y%m%d"))

    for i in range(0, dfLen):
        stockCodeDB = str(stockCode[i])
        stockNameDB = str(stockName[i])
        stockIndustryDB = str(stockIndustry[i])
        stockAreaDB = str(stockArea[i])
        stockPeDB = round(float(stockPe[i]), 4)
        stockOutstandingDB = round(float(stockOutstanding[i]), 4)
        stockTotalsDB = round(float(stockTotals[i]), 4)
        stockTotalAssetsDB = round(float(stockTotalAssets[i]), 4)
        stockLiquidAssetsDB = round(float(stockLiquidAssets[i]), 4)
        stockFixedAssetsDB = round(float(stockFixedAssets[i]), 4)
        stockReservedDB = round(float(stockReserved[i]), 4)
        stockReservedPerShareDB = round(float(stockReservedPerShare[i]), 4)
        stockEspDB = round(float(stockEsp[i]), 4)
        stockBvpsDB = round(float(stockBvps[i]), 4)
        stockPbDB = round(float(stockPb[i]), 4)
        timeToMarketDB = str(stockTimeToMarket[i])[0:4] + '-' + str(stockTimeToMarket[i])[4:6] + '-' + str(stockTimeToMarket[i])[6:8]
        stockUndpDB = round(float(stockUndp[i]), 4)
        stockPerundpDB = round(float(stockPerundp[i]), 4)
        stockRevDB = round(float(stockRev[i]), 4)
        stockProfitDB = round(float(stockProfit[i]), 4)
        stockGprDB = round(float(stockGpr[i]), 4)
        stockNprDB = round(float(stockNpr[i]), 4)
        stockHoldersDB = round(float(stockHolders[i]), 4)

        # print(stockTimeToMarket[i])
        # print(timeToMarketDB)

        try:
            cursor.execute("insert into stock_basics(code, name, industry, area, pe, outstanding, "
                       "totals, totalAssets, liquidAssets, fixedAssets, reserved, "
                       "reservedPerShare, esp, bvps, pb, timeToMarket, undp, "
                       "perundp, rev, profit, gpr, npr, holders)"
                       "values('%s', '%s', '%s', '%s', '%f', '%f', "
                       "'%f', '%f', '%f', '%f', '%f', "
                       "'%f', '%f', '%f', '%f', to_date('%s', 'yyyy-MM-dd'), '%f', "
                       "'%f', '%f', '%f', '%f', '%f', '%f')"
                       % (stockCodeDB, stockNameDB, stockIndustryDB, stockAreaDB, stockPeDB, stockOutstandingDB,
                        stockTotalsDB, stockTotalAssetsDB, stockLiquidAssetsDB, stockFixedAssetsDB, stockReservedDB,
                        stockReservedPerShareDB, stockEspDB, stockBvpsDB, stockPbDB, timeToMarketDB, stockUndpDB,
                        stockPerundpDB, stockRevDB, stockProfitDB, stockGprDB, stockNprDB, stockHoldersDB))
            cursor.execute("commit")
            print("已存入  ", i)
        except Exception:
            pass


# 获取数据库连接
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
    # cursor.execute("select * from stock_basics")
    # row = cursor.fetchone()
    # print(row)


def main():
    cursor = getConfig()
    getBasics(cursor)


if __name__ == '__main__':
    main()







