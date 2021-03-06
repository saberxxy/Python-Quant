# -*- coding: utf-8 -*-
# 计算MIC，最大信息数

import pandas as pd
import time
import uuid
import os
import numpy as np
import re
from minepy import MINE
import itertools
import math
import pymysql


# 连接数据库
def get_cursor():
	conn = pymysql.connect(host='127.0.0.1', user='root',
						   passwd='123456', db='stock', port=3306, charset='utf8')
	cursor = conn.cursor()
	return cursor


def get_stock_data(start_date, end_date):
	cursor = get_cursor()
	sql_1 = """select table_name from information_schema.`TABLES` a
               where a.TABLE_SCHEMA = 'stock'
               and (table_name like 'STOCK_6%' 
	           or table_name like 'STOCK_0%' or table_name like 'STOCK_3') 
	           and table_name <> 'STOCK_000001_SSE' and substr(table_name, 7, 10) in 
	           (select symbol from stock_basics where list_status='L') 
	           order by table_name asc
	        """
	cursor.execute(sql_1)
	res = cursor.fetchall()
	res_list = []
	for i in res:
		res_list.append(i[0])
	cursor.close()
	return res_list


# 为存储结果和记录数据建表
def create_base_table(end_date):
	cursor = get_cursor()
	# 首先判断两张表是否存在
	sql_1 = """select table_name from information_schema.`TABLES` a
	           where a.table_schema='stock' and a.table_name = \'stock_mine_result_"""  \
			+ str(end_date).replace('-','') + """\'"""
	cursor.execute(sql_1)
	a = len(cursor.fetchall())
	print(a)

	sql_2 = """select table_name from information_schema.`TABLES` a
	           where a.table_schema='stock' and a.table_name = \'stock_mine_result_ok_"""  \
			+ str(end_date).replace('-','') + """\'"""
	cursor.execute(sql_2)
	b = len(cursor.fetchall())
	print(b)

	if a == 0 or b == 0:
		if a != 0:
			cursor.execute("drop table stock.stock_mine_result_" +str(end_date).replace('-',''))
		if b != 0:
			cursor.execute("drop table stock.stock_mine_result_ok_" + str(end_date).replace('-', ''))
		# 只要有一张表不存在，重新建两张表
		sql_3 = "create table stock.stock_mine_result_" +str(end_date).replace('-','')+ \
 			" (uuid varchar(100), stock_code_1 varchar(20), stock_table_name_1 varchar(20), \
 			stock_code_2 varchar(20), stock_table_name_2 varchar(20), MIC float(20), \
 			MAS float(20), MEV float(20), MCN float(20), \
 			GMIC float(20), TIC float(20), COV float(20), CORR float(20))"
		sql_4 = "create table stock.stock_mine_result_ok_" + str(end_date).replace('-', '') + \
			"(ok_1 varchar(30), ok_2 varchar(30))"
		cursor.execute(sql_3)
		cursor.execute(sql_4)
		print('建表完毕')
	else:
		#不建表
		print('不用建表')
		pass

	cursor.close()



def ca_mic_corr(cursor, table_name_1, table_name_2, start_date, end_date):

	sql_1 = "select distinct sdate, close as close_1 " + \
			" from stock." + table_name_1 + \
			" where sdate between date_format('" + start_date + "', '%Y-%m-%d')  \
			and date_format('" + end_date + "', '%Y-%m-%d')"
	cursor.execute(sql_1)
	a = cursor.fetchall()
	result_1 = pd.DataFrame(list(a), columns=['sdate', 'close_1'])
	# print(result_1)
	sql_2 = "select distinct sdate, close as close_2 " + \
			" from stock." + table_name_2 + \
			" where sdate between date_format('" + start_date + "', '%Y-%m-%d')  \
			and date_format('" + end_date + "', '%Y-%m-%d')"
	cursor.execute(sql_2)
	b = cursor.fetchall()
	result_2 = pd.DataFrame(list(b), columns=['sdate', 'close_2'])
	x = pd.merge(result_1, result_2, on='sdate', how='inner')
	if len(x) >= 250:
	# 组合大于250个交易日
		mine = MINE(alpha=0.6, c=15, est="mic_approx")
		mine.compute_score(x['close_1'], x['close_2'])
		# 计算MIC各类指标
		mic, mas, mev, mcn, gmic, tic = return_stats(mine)

		dfab = pd.DataFrame(np.array([x['close_1'], x['close_2']]).T, columns=['A', 'B'])
		# 计算协方差及相关系数
		cov = round(float(dfab.A.cov(dfab.B)), 4)  # 协方差
		corr = round(float(dfab.A.corr(dfab.B)), 4)  # 相关系数

		# print(mic, mas, mev, mcn, gmic, tic)

		if math.isnan(corr):
			print(math.isnan(corr))
			pass
		else:
			cursor.execute("insert into stock.stock_mine_result_" + str(end_date).replace('-','') + \
							"(uuid, stock_code_1, stock_table_name_1, stock_code_2, stock_table_name_2, \
							 mic, mas, mev, mcn, gmic, tic, cov, corr) \
							 values ('%s', '%s', '%s', '%s', '%s', \
							 '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (
				str(uuid.uuid1()),
				str(table_name_1).replace("stock_", ""),
				str(table_name_1),
				str(table_name_2).replace("stock_", ""),
				str(table_name_2),
				mic, mas, mev, mcn, gmic, tic, cov, corr
			))
			cursor.execute("commit")



def return_stats(mine):
	# mic, mas, mev, mcn, gmic, tic
	return round(float(mine.mic()), 4), round(float(mine.mas()), 4), \
		round(float(mine.mev()), 4), round(float(mine.mcn(0)), 4), \
		round(float(mine.gmic()), 4), round(float(mine.tic()), 4)


if __name__ == '__main__':
	# 开始时间
	start_date = "2018-01-01"
	# 结束时间，即求取该时间范围内的数据
	end_date = '2019-02-28'
	res_list = get_stock_data(start_date, end_date)
	print(res_list)
	print(len(res_list))

	# 建表
	create_base_table(end_date)

	# 查是否已处理
	cursor = get_cursor()
	sql_1 = "select ok_1, ok_2 from stock.stock_mine_result_ok_" + str(end_date).replace('-', '')
	cursor.execute(sql_1)
	ok_list = cursor.fetchall()
	ok_group_list = []
	for i in ok_list:
		ok_group_list.append(str(i[0]) + "_" + str(i[1]))

	print(ok_group_list)

	# 两两组合
	tmp = []
	for i in itertools.combinations(res_list, r=2):
		time_1 = time.time()
		# print(i)
		# 计算相关性并入库
		if len(ok_group_list) != 0:
			if str(i[0])+"_"+str(i[1]) not in ok_group_list:
				ca_mic_corr(cursor, i[0], i[1], start_date, end_date)
				cursor.execute("insert into stock.stock_mine_result_ok_" + str(end_date).replace('-', '') + \
					"(ok_1, ok_2) values('%s', '%s')" % (str(i[0]), str(i[1])))
				cursor.execute(sql_1)
				cursor.execute("commit")
			else:
				print(str(i[0])+"_"+str(i[1]), "	已OK")
		else:
			ca_mic_corr(cursor, i[0], i[1], start_date, end_date)
			cursor.execute("insert into stock.stock_mine_result_ok_" + str(end_date).replace('-', '') + \
						   "(ok_1, ok_2) values('%s', '%s')" % (str(i[0]), str(i[1])))
			cursor.execute(sql_1)
			cursor.execute("commit")
		time_2 = time.time()
		print(i[0], i[1], "	计算入库完毕", time_2-time_1)



