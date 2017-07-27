import tushare as ts
import pandas as pd
from pandas import DataFrame, Series
import cx_Oracle

# data = ts.get_industry_classified()
# print(data[data['code'] == '000001'])
# data = ts.get_stock_basics()
# name = data[data.index=='000001'].name
# print(type(name[0]))


# data = ts.get_stock_basics()
    # df = data[data.index == '000001'].name
    #
    # data.name=df
    # print(df)


# df = ts.get_h_data('000001', start='2017-01-01')
# # for i in df.index:
# #     print(i, type(i))
#
# for j in range(df.shape[0]):
#     print(df.iloc[j], type(df.iloc[j]))
# print(len(df))


# tns = cx_Oracle.makedsn('localhost', 1521, 'orcl')
# conn = cx_Oracle.connect('stock', '123456', tns)
# print(conn.version)

def main():
    stock_code="600848"
    sql = "CREATE TABLE "+stock_code+"""
        (
                UUID VARCHAR2(80) PRIMARY KEY,
                DATETIME DATE NOT NULL,
                CODE VARCHAR2(20),
                C_NAME VARCHAR2(80),
                INDUSTRY VARCHAR2(80),
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
    print(sql)
if __name__ == '__main__':
    main()


