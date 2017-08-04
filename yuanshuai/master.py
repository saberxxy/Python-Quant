import tushare as ts
import threading
from time import ctime, sleep
import math
import get_stock_data as gsd
import format_stock_data as fsd
import oracle_connect as oc

conn = oc.conn()
cursor = conn.cursor()


# 查询数据,获得所有上市公司股票代码
def query():
    """
    查询数据库获取所有股票code
    :return:
    """
    sql = "select code from STOCK_BASICS"
    rs = cursor.execute(sql)
    result = rs.fetchall()
    tables = [i[0] for i in result]
    print(tables, type(tables))
    return tables
    # for i in result:
    #     # print(i, type(i))  # 返回元组
    #     tables.append(i[0])
    # # print(tables, type(tables))
    # return tables


def split(table_list, thread_num=3):
    """
    切分列表
    :param table_list:要切分的列表
    :param thread_num:分片个数（欲使用的线程数）
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
    if right < length:
        tbls.append(table_list[int(left):int(length)])
    print("tbls:", tbls, len(tbls))
    return tbls


# 创建表
def create(tables):
    for i in tables:
        oc.create_table(i)
        print("create table stock_"+i, ctime())


# 插入数据
def insert(tables, start=None):
    for i in tables:
        df = gsd.get_data(i, start)
        data = fsd.format_data(df)
        print("get stock_"+i, ctime())
        oc.insert_data(i, data)
        print("insert data to stock_%s, %s" % (i, ctime()))


# 处理（建表，插数）切分好的1/n数据
def create_insert(tables, start=None):
    threads = []
    t1 = threading.Thread(target=create, args=(tables,))
    threads.append(t1)
    t2 = threading.Thread(target=insert, args=(tables, start,))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()


def test(start=None, split_number=None):
    """
    全量数据导入
    :param start: 开始日期
    :param split_number:  分片数
    :return:
    """
    # rs = query()
    rs = ['002300', '002301', '002302', '002303', '002304', '002305', '002306', '002307']
    tbls = split(rs, split_number)
    for tbl in tbls:
        create_insert(tbl, start)
    print('All Over!', ctime())


def main():
    # rs = query()
    # print(len(rs))
    # rs = ['002300', '002301', '002302', '002303', '002304', '002305', '002306', '002307']
    # tbls = split(rs, 4)
    # for tbl in tbls:
    #     create_insert(tbl)
    test(start='2017-01-01', split_number=3)


if __name__ == '__main__':
    main()
