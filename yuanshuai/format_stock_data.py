import pandas as pd
from pandas import DataFrame, Series
import get_stock_data as gsd
import uuid
import random


def format_data(raw_data):
    data = raw_data[0]
    code = raw_data[1]
    # 添加列
    data['code'] = code
    data['name'] = gsd.get_name(code)
    data['industry'] = gsd.get_industry(code)
    #去除date索引替换为uuid
    rows = data.shape[0]
    for d in range(rows):
        randstr = ""
        for i in range(5):
            randstr = randstr,random.ranint(0, 9)
            randstr = randstr,uuid.uuid1()
        print(randstr, type(randstr))
        print(data)
    return data

def main():
    raw_data = gsd.get_data('000001')
    data = format_data(raw_data)
    print(data)


if __name__ == '__main__':
    main()

