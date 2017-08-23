# -*- coding: utf-8 -*-
import get_stock_data as gsd
import format_stock_data as fsd
import oracle_connect as oc
import multiprocessing as mp
from multiprocessing import Process
from time import ctime, sleep

conn = oc.connect()


def task(code, start):
    print("股票", code, "开始")
    try:
        rawData = gsd.get_data(code=code, start=start)
    except:
        # 获取数据异常则直接结束，不建表
        print("FETCH STOCK %d ERROR" %(code))
        return
    data = fsd.format_data(rawData)
    # print(ctime())
    if not oc.is_table_exist(conn, code):  # 如果表不存在，先创建表
        oc.create_table(conn, code)  # 如果表不存在，先建表
    try:
        insert_data = oc.insert_data(conn, code, data)
    except:
        oc.truncate_table(conn, code)
        print("股票", code, "插入异常")
    # print(ctime())
    print("股票", code, "结束")


# def main():
#     # list = gsd.query(conn)
#     list = ['002301', '002302', '002303', '002304', '002305', '002306', '002307']
#
#     # left_list = [i for i in list if i not in solved_list]
#
#     # list = ['002306', '002307']
#     solved_list = oc.all_solved_tables(conn)
#     left_list = set(list).difference(set(solved_list))
#     count = 0
#     while len(left_list) != 0:
#         count = count + 1
#         print('left_list: ', left_list)
#         pool = mp.Pool(processes=8)
#         print("程序开始：================", ctime())
#
#         for i in left_list:
#             code = i
#             # print(code, type(code))
#             pool.apply_async(task, (code, '2015-01-01'))
#
#         pool.close()
#         pool.join()  # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
#         solved_list = oc.all_solved_tables(conn)
#         left_list = set(list).difference(set(solved_list))
#         if count > 5:
#             break
#     print("程序结束：================", ctime())


def main():  # 单进程
    list = gsd.query(conn)
    # list = ['002301', '002302', '002303', '002304', '002305', '002306', '002307']
    # left_list = [i for i in list if i not in solved_list]
    # list = ['002306', '002307']
    solved_list = oc.all_solved_tables(conn)
    left_list = set(list).difference(set(solved_list))
    count = 0
    print("程序开始：================", ctime())
    while len(left_list) != 0:
        count = count + 1
        print('left_list: ', left_list)
        for i in left_list:
            code = i
            # print(code, type(code))
            task(code, '2015-01-01')
            sleep(5)
        solved_list = oc.all_solved_tables(conn)
        left_list = set(list).difference(set(solved_list))
        if count > 5:
            break
    print("程序结束：================", ctime())


if __name__ == '__main__':
    main()
