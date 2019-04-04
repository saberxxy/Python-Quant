# -*- coding: utf-8 -*-
# 生成所有股票的组合

import pandas as pd
import time
import datetime
import os
import numpy as np
import itertools
import pymysql
import math


# 连接数据库
def get_cursor():
	conn = pymysql.connect(host='127.0.0.1', user='root',
						   passwd='123456', db='stock', port=3306, charset='utf8')
	cursor = conn.cursor()
	return cursor


# 获取股票列表
def get_stock_data(cursor):
	sql_1 = """select table_name from information_schema.`TABLES` a
               where a.TABLE_SCHEMA = 'stock'
               and (table_name like 'STOCK_6%' 
	           or table_name like 'STOCK_0%' or table_name like 'STOCK_3') 
	           and table_name <> 'STOCK_000001_SSE' and substr(table_name, 7, 10) in 
	           (select symbol from stock_basics where list_status='L') 
	           order by table_name desc
	        """
	cursor.execute(sql_1)
	res = cursor.fetchall()
	res_list = []
	for i in res:
		res_list.append(i[0])
	return res_list


# 生成组合并入库
def make_iter(cursor, res_list):
	sql_1 = """select table_name from information_schema.`TABLES`
	           where TABLE_SCHEMA = 'stock'
	           and table_name = 'stock_cp' """
	cursor.execute(sql_1)
	table_exists = cursor.fetchall()
	# print(len(table_exists))
	if len(table_exists) == 0:
		print(False)
		sql_1 = """drop table if exists stock.stock_cp"""
		cursor.execute(sql_1)
		sql_2 = """create table stock.stock_cp(
				   stock_table_name_1 varchar(30),
				   stock_table_name_2 varchar(30))"""
		cursor.execute(sql_2)

	cp = itertools.combinations(res_list, r=2)
	count = 0
	time_1 = time.time()
	cp_tuple = tuple(cp)
	# print(cp_tuple)
	# 批量插入
	cursor.executemany("""insert into stock.stock_cp(stock_table_name_1, stock_table_name_2)
			              values(%s, %s)
			          """, cp_tuple)
	cursor.execute("commit")


if __name__ == '__main__':
	cursor = get_cursor()

	res_list = get_stock_data(cursor)

	make_iter(cursor, res_list)

	cursor.close()

