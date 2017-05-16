#-*- coding=utf-8 -*-
#获取上市公司信息、获取一百支股票信息


import tushare as ts
import pymysql
import random

"""数据库连接相关"""
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                       db=mysqlDatabaseName, port=mysqlPort, charset="utf8")


#获取上市公司信息
def getCompany():
    """code 代码, name 名称, industry 所属行业, area 地区, pe 市盈率, outstanding 流通股本(亿), totals 总股本(亿),
    totalAssets 总资产(万), liquidAssets 流动资产, fixedAssets 固定资产, reserved 公积金, reservedPerShare 每股公积金,
    esp 每股收益, bvps 每股净资, pb 市净率, timeToMarket 上市日期, undp 未分利润, perundp 每股未分配, rev 收入同比(%),
    profit 利润同比(%), gpr 毛利率(%), npr 净利润率(%), holders 股东人数 """
    try:
        basics = ts.get_stock_basics()
        index = list(basics.index)  #股票代码
        name = list(basics.name)  #股票名称
        industry = list(basics.industry)  #所属行业
        indexLen = len(index)

        cur1 = conn.cursor()
        cur1.execute("DROP TABLE IF EXISTS basics;")  #建立保存上市公司信息的表
        sqlCreate = """create table basics
                       (b_id int comment '主键',
                       b_index varchar(100) comment '股票代码',
                       b_name varchar(100) comment '股票名称',
                       b_industry varchar(100) comment '所属行业',
                       PRIMARY key(b_id))"""
        cur1.execute(sqlCreate)

        for i in range(0, indexLen):
            print (i+1, index[i], name[i], industry[i])

            cur1.execute(
                "INSERT INTO basics(b_id, b_index, b_name, b_industry) "
                "VALUES('%d', '%s', '%s', '%s');"
                 % (i+1, index[i], name[i], industry[i]))

        cur1.execute('commit;')
        print ('插入完毕！')
    except Exception:
        pass


def getCompanyShares():
    #随机挑选一百支非金融行业的股票
    try:
        cur1 = conn.cursor()
        cur1.execute("select b_index, b_name from basics "
                     "where (b_industry not like '%证券%') and (b_industry not like '%银行%') "
                     "and (b_industry not like '%金融%') and (b_industry not like '%保险%');")
        # print (type(cur1.fetchall()))
        bIndex = []
        for i in cur1.fetchall():
            # print (i[0], i[1])
           bIndex.append(i[0]+'_'+i[1])

        # print (bIndex)
        # slice = random.sample(bIndex, 100)
        print (len(bIndex))
        count = 1
        for i in bIndex:
            # print ("======================================="+i)
            number = i.split('_')[0]
            name = i.split('_')[1]

            if '*' in name:  #去掉名字中的‘*’
                name = name.replace('*', '')
            if ' ' in name:  #去掉名字中的空格
                name = name.replace(' ', '')

            i = i + "_ORG"
            # print ("++++++++++++++++++++++++++++++++++++++++"+i)

            data = ts.get_hist_data(code=number, start='2016-01-01', end='2016-12-31')
            print ("-------------------------------")
            if len(data.index) == 242:
                date = list(data.index)  # 日期
                open = list(data.open)  # 开盘价
                high = list(data.high)  # 最高价
                close = list(data.close)  # 收盘价
                low = list(data.low)  # 最低价
                change = list(data.p_change)  # 收益率

                cur1.execute("DROP TABLE IF EXISTS %s;" % (i))  # 建立保存股票数据的表
                cur1.execute("""create table %s
                               (b_id int comment '主键',
                                b_index varchar(100) comment '股票代码',
                                b_name varchar(100) comment '股票名称',
                                b_date date comment '日期',
                                b_open float comment '开盘价',
                                b_high float comment '最高价',
                                b_close float comment '收盘价',
                                b_low float comment '最低价',
                                b_change float comment '涨跌幅',
                                PRIMARY key(b_id));""" % (i))

                for j in range(0, len(date)):
                    # print (j + 1, number, name, date[j], open[j], high[j], close[j], low[j], change[j])
                    cur1.execute("INSERT INTO %s(b_id, b_index, b_name, b_date, b_open, b_high, b_close, b_low, b_change) "
                                 "VALUES('%d', '%s', '%s', '%s', '%f', '%f', '%f', '%f', '%f');"
                                 % (i, j+1, number, name, date[j], open[j], high[j], close[j], low[j], change[j]))
                cur1.execute("commit;")
                print (u'第%d支股票导入完毕' % (count))
                count += 1
            else:
                pass
    except Exception:
        pass

def getSzzs():
    try:
        data = ts.get_hist_data(code='000001', start='2016-01-01', end='2016-12-31')
        # print (data)
        number = '000001'
        name = '上证指数'
        date = list(data.index)  # 日期
        open = list(data.open)  # 开盘价
        high = list(data.high)  # 最高价
        close = list(data.close)  # 收盘价
        low = list(data.low)  # 最低价
        change = list(data.p_change)  # 收益率
        i = '000001_上证指数_org'

        cur1 = conn.cursor()
        cur1.execute("DROP TABLE IF EXISTS %s;" % (i))  # 建立保存股票数据的表
        cur1.execute("""create table %s
                        (b_id int comment '主键',
                        b_index varchar(100) comment '股票代码',
                        b_name varchar(100) comment '股票名称',
                        b_date date comment '日期',
                        b_open float comment '开盘价',
                        b_high float comment '最高价',
                        b_close float comment '收盘价',
                        b_low float comment '最低价',
                        b_change float comment '涨跌幅',
                        PRIMARY key(b_id));""" % (i))
        for j in range(0, len(date)):
            # print (j + 1, number, name, date[j], open[j], high[j], close[j], low[j], change[j])
            cur1.execute("INSERT INTO %s(b_id, b_index, b_name, b_date, b_open, b_high, b_close, b_low, b_change) "
                         "VALUES('%d', '%s', '%s', '%s', '%f', '%f', '%f', '%f', '%f');"
                         % (i, j + 1, number, name, date[j], open[j], high[j], close[j], low[j], change[j]))
        cur1.execute("commit;")
    except Exception:
        pass



def main():
    getCompany()
    getCompanyShares()
    getSzzs()
    print ("数据采集完毕！")


if __name__ == '__main__':
    main()

