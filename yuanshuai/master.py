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
    :param thread_num:要使用的线程数
    :return:
    """
    # length = len(table_list)
    length = 33
    index_num = length / thread_num
    positions = [] # 存储分片位置
    position = index_num if index_num % 1 == 0 else math.ceil(index_num)
    while position < length:
        position = position + position
        positions.append(position)
    print(positions, type(positions))
    # print("position:", position, type(position))
    tbls=[]
    for i in positions:
        left = 0
        right = i
        tbl = table_list[left: right]
        tbls.append(tbl)
        left = right
    print(tbls, len(tbls))
    tbl1 = table_list[0:position]
    tbl2 = table_list[position:length]
    return tbl1, tbl2


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
