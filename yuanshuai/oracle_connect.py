# -*- coding:utf-8 -*-
import cx_Oracle
import get_stock_data as gsd
import format_stock_data as fsd
import time
import re


# 连接数据库
def connect(username='stock', password='123456', host='localhost', port=1521, sid='orcl'):
    tns = cx_Oracle.makedsn(host, port, sid)
    con = cx_Oracle.connect(username, password, tns)  # 连接数据库
    print('connect successfully! the Oracle version:', con.version)  # 打印版本号
    # cursor = conn.cursor()  # 创建cursor
    # return cursor
    return con


# 判断表是否已存在
def is_table_exist(conn, code):
    table = "STOCK_"+code
    cursor = conn.cursor()
    sql = "select table_name from user_tables"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    print(tables, type(tables))
    return tables.__contains__(table)


# 删除一张表
def delete_table(conn, code):
    table = "STOCK_" + code
    cursor = conn.cursor()
    sql = "drop table "+table
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print('表STOCK_',code,' 删除失败')


# 查看当前所有表（即已处理的）
def all_solved_tables(conn):
    cursor = conn.cursor()
    sql = "select table_name from user_tables"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    tables = [i for i in tables if re.match(r'STOCK_\d{6}', i) is not None]  # 用正则筛选交易数据表
    return tables


# 建表
def create_table(conn, stock_code):
    """
    创建空表
    :param stock_code:股票代码
    :return:
    """
    cursor = conn.cursor()
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
    # print("执行的sql语句:\n", sql)
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
        # print(i)
        cursor.execute(i)
        # TODO: 不需要commit?
        # print("stock_"+stock_code+"表已创建", time.ctime())


# 插入数据
def insert_data(conn, stock_code, stock_data):
    cursor = conn.cursor()
    """
    向表中插入数据
    :param stock_code:股票代码
    :param stock_data: 股票数据
    :return:
    """
    # con = conn()
    # cursor = con.cursor()
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
    # print(rows, type(rows))
    try:

        sql = "insert into STOCK_" + stock_code + "(uuid, \"DATE\", code, name, classify, open, close, high, low, " \
                                              "volume, amount, y_close, p_change, p_change_rate)values(:uuid, " \
                                              "to_date(:datex, 'yyyy-mm-dd'), :code, :namex, :classify, :openx, " \
                                              ":closex, :high, :low, :volume, :amount, :y_close, :p_change, " \
                                              ":p_change_rate) "
        cursor.prepare(sql)
        cursor.executemany(sql, rows)
        conn.commit()
    except Exception:
        print("Error")


def main():
    # create_table('000004')
    # data1 = gsd.get_data('000004')
    # data2 = fsd.format_data(data1)
    # insert_data('000004', data2)
    # all_company()
    # query_columns('000001', ('name', 'age', 'tel'))
    conn = connect()
    print(all_solved_tables(conn))
    # print(delete_table(conn, '002300'))


if __name__ == '__main__':
    main()
