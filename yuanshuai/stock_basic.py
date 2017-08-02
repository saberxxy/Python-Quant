import cx_Oracle
import get_stock_data as gsd
from pypinyin import pinyin
import pypinyin

conn = cx_Oracle.connect('stock/123456@localhost:1521/orcl')


def all_company():
    """
    存储所有上市公司表信息
    :return:
    """

    # TODO: 建表语句用SQL脚本 or 代码执行
    cursor = conn.cursor()
    df = gsd.get_all_company()

    stockCode = list(df.index)  # 股票代码
    stockName = list(df['name'])  # 股票名称
    stockIndustry = list(df['industry'])  # 所属行业
    stockArea = list(df['area'])  # 所在区域
    stockPe = list(df['pe'])  # 市盈率
    stockOutstanding = list(df['outstanding'])  # 流通股本(亿)
    stockTotals = list(df['totals'])  # 总股本(亿)
    stockTotalAssets = list(df['totalAssets'])  # 总资产(万)
    stockLiquidAssets = list(df['liquidAssets'])  # 流动资产
    stockFixedAssets = list(df['fixedAssets'])  # 固定资产
    stockReserved = list(df['reserved'])  # 公积金
    stockReservedPerShare = list(df['reservedPerShare'])  # 每股公积金
    stockEsp = list(df['esp'])  # 每股收益
    stockBvps = list(df['bvps'])  # 每股净资
    stockPb = list(df['pb'])  # 市净率
    stockTimeToMarket = list(df['timeToMarket'])  # 上市日期
    stockUndp = list(df['undp'])  # 未分利润
    stockPerundp = list(df['perundp'])  # 每股未分配
    stockRev = list(df['rev'])  # 收入同比(%)
    stockProfit = list(df['profit'])  # 利润同比(%)
    stockGpr = list(df['gpr'])  # 毛利率(%)
    stockNpr = list(df['npr'])  # 净利润率(%)
    stockHolders = list(df['holders'])  # 股东人数

    dfLen = len(df)

    # print(time.strptime(stockTimeToMarket[1], "%Y%m%d"))

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
        timeToMarketDB = str(stockTimeToMarket[i])[0:4] + '-' + str(stockTimeToMarket[i])[4:6] + '-' + str(
            stockTimeToMarket[i])[6:8]
        stockUndpDB = round(float(stockUndp[i]), 4)
        stockPerundpDB = round(float(stockPerundp[i]), 4)
        stockRevDB = round(float(stockRev[i]), 4)
        stockProfitDB = round(float(stockProfit[i]), 4)
        stockGprDB = round(float(stockGpr[i]), 4)
        stockNprDB = round(float(stockNpr[i]), 4)
        stockHoldersDB = round(float(stockHolders[i]), 4)

        a = str(pinyin(stockNameDB, style=pypinyin.FIRST_LETTER))
        stockTableNameDB = "".join(a).replace('[', '').replace(']', '').replace("'", '').replace(',', ''). \
                               replace(' ', '').replace('*', '').upper() + stockCodeDB
        # print(stockTableNameDB)

        # print(stockTimeToMarket[i])
        # print(timeToMarketDB)
        #
        try:
            cursor.execute("insert into stock_basics(code, name, industry, area, pe, outstanding, "
                           "totals, totalAssets, liquidAssets, fixedAssets, reserved, "
                           "reservedPerShare, esp, bvps, pb, timeToMarket, undp, "
                           "perundp, rev, profit, gpr, npr, holders, tablename)"
                           "values('%s', '%s', '%s', '%s', '%f', '%f', "
                           "'%f', '%f', '%f', '%f', '%f', "
                           "'%f', '%f', '%f', '%f', to_date('%s', 'yyyy-MM-dd'), '%f', "
                           "'%f', '%f', '%f', '%f', '%f', '%f', '%s')"
                           % (stockCodeDB, stockNameDB, stockIndustryDB, stockAreaDB, stockPeDB, stockOutstandingDB,
                              stockTotalsDB, stockTotalAssetsDB, stockLiquidAssetsDB, stockFixedAssetsDB,
                              stockReservedDB,
                              stockReservedPerShareDB, stockEspDB, stockBvpsDB, stockPbDB, timeToMarketDB, stockUndpDB,
                              stockPerundpDB, stockRevDB, stockProfitDB, stockGprDB, stockNprDB, stockHoldersDB,
                              stockTableNameDB))
            cursor.execute("commit")
            print("已存入  ", i)
        except Exception:
            print("Error")


def main():
    all_company()


if __name__ == '__main__':
    main()
