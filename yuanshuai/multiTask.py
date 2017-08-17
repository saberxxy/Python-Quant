# -*- coding: utf-8 -*-
import get_stock_data as gsd
import format_stock_data as fsd
import oracle_connect as oc
import multiprocessing as mp
from multiprocessing import Process
from time import ctime

conn = oc.connect()


def task(code, start):
    print("股票", code, "开始")
    rawData = gsd.get_data(code=code, start='2017-01-01')
    data = fsd.format_data(rawData)
    # print(ctime())
    if(oc.is_table_exist(conn, code)):  # 如果表已存在，直接插入数据
        pass
    else:
        oc.create_table(conn, code)  # 如果表不存在，先建表
    try:
        oc.insert_data(conn, code, data)
    except:
        oc.delete(conn, code)  # TODO: 插入数据失败则删除表重建,必要吗？？
    # print(ctime())
    print("股票", code, "结束")


def main():
    # list = gsd.query(conn)
    list = ['002300', '002301', '002302', '002303', '002304', '002305', '002306', '002307']

    pool = mp.Pool(processes=8)
    print("开始：================",ctime())

    for i in list:
        code = i
        # print(code, type(code))
        pool.apply_async(task, (code, '2017-01-01'))

    pool.close()
    pool.join()  # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    print("结束：================", ctime())

        # # 仍有表未处理完，则继续
        # solved_tables = oc.all_solved_tables(conn)
        # tmp = [t for t in list if t not in solved_tables]  # 未处理的表
        # if tmp == []:
        #     break




if __name__ == '__main__':
    main()