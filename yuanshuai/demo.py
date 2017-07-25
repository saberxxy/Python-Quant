import tushare as ts
import pandas as pd
from pandas import DataFrame, Series


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
def main():
    df = ts.get_h_data('000001', start='2017-01-01')
    # for i in df.index:
    #     print(i, type(i))

    for j in range(df.shape[0]):
        print(df.iloc[j], type(df.iloc[j]))

if __name__ == '__main__':
    main()


