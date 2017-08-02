import threading
import random
from time import ctime, sleep
import math


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
        # if right == positions[-1]:
        #     if right < length:
        #         tbls.append(table_list[int(left):int(length)])

    if right < length:
        tbls.append(table_list[int(left):int(length)])
    print("tbls:", tbls, len(tbls))
    return tbls


def create(tables):
    for i in tables:
        print("create table stock_"+i, ctime())
        sleep(1)


def insert(tables):
    for i in tables:
        print("get stock_"+i, ctime())
        print("insert data to stock_%s, %s" % (i, ctime()))
        sleep(3)


# 执行切分好的1/n数据
def create_insert(tables):
    threads = []
    t1 = threading.Thread(target=create, args=(tables,))
    threads.append(t1)
    t2 = threading.Thread(target=insert, args=(tables,))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()


def main():
    tables = []
    for i in range(15):
        tables.append(str(i))
    tbls = split(tables, 3)
    for tbl in tbls:
        # TODO: 初步多线程，如何提速？线程外套线程如何写？or多进程
        threads = []
        th = threading.Thread(target=create_insert, args=(tbl,))
        threads.append(th)
        for t in threads:
            t.setDaemon(True)
            t.start()
        t.join()
        # create_insert(tbl)
    print("all over! %s" % (ctime()))


if __name__ == '__main__':
    main()
