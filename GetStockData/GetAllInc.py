# -*- coding=utf-8 -*-
# 获取股票增量历史记录

"""
1、查询数据库中STOCK_XXXXXX的所有表，提取出股票代码与最大日期
2、将系统日期与最大日期比对，一头一尾作为提取该股票CSV文件的入参
3、取数，解析并入库（去掉清空表的步骤）
"""

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
from multiprocessing import Pool
import uuid
import os
import numpy as np


# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()


# 查询数据库中STOCK_XXXXXX的所有表，提取出股票代码与最大日期
def getTable():
    # 提取股票代码
    sql = "select table_name from user_tables where regexp_like(table_name, 'STOCK_\d')"
    tableName = cursor.execute(sql).fetchall()
    tableName = str(tableName).replace("('", "").replace("',)", "").replace("[", "").replace("]", "")
    tableNameList = tableName.split(", ")
    # print(tableName)
    # code = [ "STOCK_"+str(i) for i in code]

    # 提取最大日期
    codeList = []
    maxDateList = []
    for i in tableNameList:
        sqlStr = "select to_char(max(sdate), 'yyyyMMdd') from "+ str(i)
        maxDate = str(cursor.execute(sqlStr).fetchall()).replace("('", "").replace("',)", "").replace("[", "").replace("]", "")
        codeList.append(str(i).replace("STOCK_", ""))
        maxDateList.append(maxDate)

    # 将两个list合为字典
    dictionary = dict(zip(codeList, maxDateList))
    return dictionary


# 将系统日期与最大日期比对，一头一尾作为提取该股票CSV文件的入参
def getStockDataInc(code, maxate):
    pass


def main():
    dictionary = getTable()
    print(dictionary)


if __name__ == '__main__':
    main()