# -*- coding=utf-8 -*-
# 获取上市公司信息
# 获取上市公司信息，更新新接口的信息

import tushare as ts
import uuid
import pandas as pd
import pymysql
import time


# 连接数据库
def get_cursor():
    conn = pymysql.connect(host='127.0.0.1', user='root',
                           passwd='123456', db='stock', port=3306, charset='utf8')
    cursor = conn.cursor()
    return cursor


def main():
    time_1 = time.time()
    pro = ts.pro_api()
    df = pro.stock_basic(fields='ts_code, symbol, name, area, industry, fullname, \
                                  enname, market, exchange, curr_type, list_status, \
                                  list_date, delist_date, is_hs')

    ts_code = list(df['ts_code'])
    symbol = list(df['symbol'])
    name = list(df['name'])
    area = list(df['area'])
    industry = list(df['industry'])
    fullname = list(df['fullname'])
    enname = list(df['enname'])
    market = list(df['market'])
    exchange = list(df['exchange'])
    curr_type = list(df['curr_type'])
    list_status = list(df['list_status'])
    list_date = list(df['list_date'])
    delist_date = list(df['delist_date'])
    is_hs = list(df['is_hs'])

    dfLen = len(df)
    # print(df.head())

    cursor = get_cursor()
    sql_1 = "drop table if exists stock.stock_basics"
    cursor.execute(sql_1)
    sql_2 = "create table stock.stock_basics \
            (uuid varchar(100), \
            ts_code varchar(100), \
            symbol varchar(100),  \
            name varchar(100), \
            area varchar(100), \
            industry varchar(100), \
            fullname varchar(100), \
            enname varchar(100), \
            market varchar(100), \
            exchange varchar(100), \
            curr_type varchar(100), \
            list_status varchar(100), \
            list_date varchar(100), \
            delist_date varchar(100), \
            is_hs varchar(100))"
    cursor.execute(sql_2)

    print("建表成功")
    for i in range(0, dfLen):
        ts_code_db = str(ts_code[i])
        symbol_db = str(symbol[i])
        name_db = str(name[i])
        area_db = str(area[i])
        industry_db = str(industry[i])
        fullname_db = str(fullname[i])
        enname_db = str(enname[i]).replace("'", "")
        market_db = str(market[i])
        exchange_db = str(exchange[i])
        curr_type_db = str(curr_type[i])
        list_status_db = str(list_status[i])
        list_date_db = str(list_date[i])
        delist_date_db = str(delist_date[i])
        is_hs_db = str(is_hs[i])
        # print(ts_code_db)


        cursor.execute("insert into stock.stock_basics values('%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s')"
                % (str(uuid.uuid1()), ts_code_db, symbol_db, name_db,
                area_db, industry_db, fullname_db, enname_db, market_db,
                exchange_db, curr_type_db, list_status_db, list_date_db,
                delist_date_db, is_hs_db ))
    cursor.execute("commit")
    time_2 = time.time()
    cursor.close()
    print("插入完毕", time_2-time_1)


if __name__ == '__main__':
    main()






