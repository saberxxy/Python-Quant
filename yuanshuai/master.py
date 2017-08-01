import tushare as ts
import cx_Oracle
import threading
from time import ctime, sleep
import math

# 要操作的表的列表
tables = []


def connect():
    conn = cx_Oracle.connect('stock/123456@localhost:1521/orcl')
    cursor = conn.cursor()
    return cursor


# 查询数据
def query(cursor):
    """
    查询数据库获取所有股票code
    :param cursor:
    :return:
    """
    sql = "select code from STOCK_BASICS"
    rs = cursor.execute(sql)
    result = rs.fetchall()

    for i in result:
        # print(i, type(i))  # 返回元组
        tables.append(i[0])
    # print(tables, type(tables))
    return tables


def split(table_list, thread_num=3):
    """
    切分列表
    :param table_list:要切分的列表
    :param thread_num:切割为多少份（欲使用的线程数）
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


def insert(tbl):
    for i in tbl:
        print("insert table %s, %s" % (i, ctime()))
        sleep(1)


def multi(tbls):
    threads = []
    t1 = threading.Thread(target=insert, args=(tbls[0],))
    threads.append(t1)
    t2 = threading.Thread(target=insert, args=(tbls[1],))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()
    print('all over!', ctime())


def main():
    cur = connect()
    rs = query(cur)
    split(rs)
    # multi(tbls)
    # print(tbl2)


if __name__ == '__main__':
    main()
