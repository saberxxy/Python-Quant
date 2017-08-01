import tushare as ts
import pandas as pd
from pandas import DataFrame, Series
import cx_Oracle
import math
import random
from time import ctime, sleep
import threading

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


def music(func):
    for i in range(2):
        print("I was listening to %s. %s" % (func, ctime()))
        sleep(1)


def movie(func):
    for i in range(2):
        print("I was at the movies %s. %s" %(func, ctime()))
        sleep(5)


def multi():
    threads = []
    t1 = threading.Thread(target=music, args=(u'爱情买卖',))
    threads.append(t1)
    t2 = threading.Thread(target=movie, args=(u'阿凡达',))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()
    print(ctime())


def split(table_list, thread_num=3):
    """
    切分列表
    :param table_list:要切分的列表
    :param thread_num:要使用的线程数
    :return:
    """
    # length = len(table_list)
    length = len(table_list)
    index_num = length / thread_num
    positions = []  # 存储分片位置
    position = index_num if index_num % 1 == 0 else math.ceil(index_num)

    step = position  # 每次叠加的索引数
    while True:
        positions.append(position)
        position = position + step
        if position > length:
            break
    print("positions:", positions, type(positions))

    tbls = []  # 分割后列表
    left = 0
    for i in positions:
        right = i
        tbl = table_list[int(left): int(right)]
        tbls.append(tbl)
        left = right
        if right == positions[-1]:
            if right < length:
                tbls.append(table_list[int(left):int(length)])
    print("tbls:", tbls, len(tbls))
    return tbls


def main():
    data = []
    for i in range(0, 33):
        n = random.randint(0, 100)
        data.append(n)
    print(data)
    split(data, 11)


if __name__ == '__main__':
    main()


