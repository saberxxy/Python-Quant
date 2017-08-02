# -*- coding:utf-8 -*-
import cx_Oracle
import get_stock_data as gsd
import format_stock_data as fsd
from pypinyin import pinyin
import pypinyin


# 连接数据库
def conn(username='stock', password='123456', host='localhost', port=1521, sid='orcl'):
    tns = cx_Oracle.makedsn(host, port, sid)
    con = cx_Oracle.connect(username, password, tns)  # 连接数据库
    print('connect successfully! the Oracle version:', con.version)  # 打印版本号
    # cursor = conn.cursor()  # 创建cursor
    # return cursor
    return con


# 建表
def create_table(stock_code):
    """
    创建空表
    :param stock_code:股票代码
    :return:
    """
    cursor = conn().cursor()
    sql = "CREATE TABLE STOCK_" + stock_code + """
    (
            UUID VARCHAR2(80) PRIMARY KEY,
            "DATE" DATE NOT NULL,
            CODE VARCHAR2(20),
            NAME VARCHAR2(80),
            CLASSIFY VARCHAR(80),
            OPEN NUMBER(20, 2),
            CLOSE NUMBER(20, 2),
            HIGH NUMBER(20, 2),
            LOW NUMBER(20, 2),
            VOLUME NUMBER(20, 1),
            AMOUNT NUMBER(20, 1),
            Y_CLOSE NUMBER(20, 2),
            P_CHANGE NUMBER(20, 2),
            P_CHANGE_RATE NUMBER(20, 6)
        )
    """
    print("执行的sql语句:\n", sql)
    cursor.execute(sql)
    # 添加注释
    comments = ["COMMENT ON TABLE STOCK_" + stock_code + " IS '" + stock_code + "'",  # 表注释
                "COMMENT ON COLUMN STOCK_" + stock_code + ".UUID IS 'UUID'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".\"DATE\" IS '日期'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".CODE IS '股票代码'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".NAME IS '股票名称'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".CLASSIFY IS '类别'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".OPEN IS '开盘价'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".CLOSE IS '收盘价'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".HIGH IS '最高价'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".LOW IS '最低价'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".VOLUME IS '成交量'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".AMOUNT IS '成交金额'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".Y_CLOSE IS '昨收盘'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".P_CHANGE IS '涨跌额'",
                "COMMENT ON COLUMN STOCK_" + stock_code + ".P_CHANGE_RATE IS '涨跌幅'"]
    for i in comments:
        print(i)
        cursor.execute(i)


# 插入数据
def insert_data(stock_code, stock_data):
    con = conn()
    cursor = con.cursor()
    rows = []
    for i in stock_data.index:
        uuid = stock_data.loc[i, 'uuid']
        date = stock_data.loc[i, 'date']
        code = stock_data.loc[i, 'code']
        name = stock_data.loc[i, 'name']
        classify = stock_data.loc[i, 'classify']
        open = stock_data.loc[i, 'open']
        close = stock_data.loc[i, 'close']
        high = stock_data.loc[i, 'high']
        low = stock_data.loc[i, 'low']
        volume = stock_data.loc[i, 'volume']
        amount = stock_data.loc[i, 'amount']
        y_close = stock_data.loc[i, 'y_close']
        p_change = stock_data.loc[i, 'p_change']
        p_change_rate = stock_data.loc[i, 'p_change_rate']
        row = (uuid, date, code, name, classify, open, close, high, low, volume, amount, y_close, p_change,
               p_change_rate)
        rows.append(row)
        print(close, type(close))
    # print(rows, type(rows))

    sql = "insert into STOCK_" + stock_code + "(uuid, \"DATE\", code, name, classify, open, close, high, low, " \
                                              "volume, amount, y_close, p_change, p_change_rate)values(:uuid, " \
                                              "to_date(:datex, 'yyyy-mm-dd'), :code, :namex, :classify, :openx, " \
                                              ":closex, :high, :low, :volume, :amount, :y_close, :p_change, " \
                                              ":p_change_rate) "
    print(sql)
    cursor.prepare(sql)
    cursor.executemany(sql, rows)
    con.commit()


def all_company():
    """
    存储所有上市公司表信息
    :return:
    """

    # TODO: 建表语句用SQL脚本 or 代码执行
    cursor = conn().cursor()
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
    # create_table('000004')
    data1 = gsd.get_data('000004')
    data2 = fsd.format_data(data1)
    insert_data('000004', data2)
    # all_company()
    # query_columns('000001', ('name', 'age', 'tel'))


if __name__ == '__main__':
    main()
