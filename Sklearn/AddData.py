#-*- coding=utf-8 -*-

import tushare as ts
from pandas import Series, DataFrame
import pandas as pd
import uuid
import re
import time


all_gp = ts.get_industry_classified()
#获取历史数据
def getData(code=None, start=None, end=None):
    data = ts.get_k_data(code=code, start=start, end=end)
    # print (data)
    data.code = code
    current_gp = all_gp[all_gp.code==code]
    data['name'] = re.findall(".*\d(.*)name.*", str(str(current_gp['name']).replace(" ", "").replace("\nName:name,dtype:object", "name")))

    #print (data['name'])

    # print(current_gp['name'])
    return data



# 添加uuid字符串
def addColumns(data):
    #data = DataFrame(data, columns=['uuid', 'date', 'open', 'close', 'high', 'low', 'volume', 'code', 'name'])
    data = DataFrame(data, columns=['uuid', 'date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume'])
    for i in data.index:
        data.loc[i, ['uuid']] = str(uuid.uuid1()) + "_" + data.loc[i].date
    print(data['name'])
    return data


def main():
    data = getData('600848', start='2017-02-03', end='2017-02-08')
    #addColumns(data)


if __name__ == '__main__':
    main()
