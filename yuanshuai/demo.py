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
    pass
if __name__ == '__main__':
    main()


