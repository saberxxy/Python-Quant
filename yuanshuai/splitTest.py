import math
import random
from time import ctime, sleep
import threading


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


def main():
    data = []
    for i in range(0, 33):
        n = random.randint(0, 100)
        data.append(n)
    print(data)
    split(data, 2)


if __name__ == '__main__':
    main()


