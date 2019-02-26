# -*- coding: utf-8 -*-
# 计算MIC，最大信息数

import pandas as pd
import time
import uuid
import os
import numpy as np
import re
from minepy import MINE
# 导入连接文件
import sys
sys.path.append('..')
import common.GetOracleConn as conn
import itertools
import math


def get_stock_data(start_date, end_date):
	cursor = conn.getConfig()
	sql_1 = "select table_name from user_tables where (table_name like 'STOCK_6%' \
	          or table_name like 'STOCK_0%' or table_name like 'STOCK_3') \
	          and table_name <> 'STOCK_000001_SSE' and substr(table_name, 7, 10) in \
	          (select symbol from stock_basics_2 where list_status='L') "
	cursor.execute(sql_1)
	res = cursor.fetchall()
	res_list = []
	for i in res:
		sql_2 = "select count(*) as my_count " + \
				" from " + i[0] + \
				" where sdate between to_date('" + start_date + "', 'yyyy-MM-dd') " + \
				" and to_date('" + end_date + "', 'yyyy-MM-dd')"
		cursor.execute(sql_2)
		# 选取大于250个交易日的股票，否则无法有足够的数据得出结论
		if cursor.fetchall()[0][0] > 250:
			res_list.append(i[0])
	cursor.close()
	return res_list


# 为存储结果和记录数据建表
def create_base_table(end_date):
	cursor = conn.getConfig()
	# 首先判断两张表是否存在
	sql_1 = "select count(*) from user_tables where table_name = \
		upper('stock_mine_result_" + str(end_date).replace('-','') + "')"
	cursor.execute(sql_1)
	a = cursor.fetchall()[0][0]

	sql_2 = "select count(*) from user_tables where table_name = \
		upper('stock_mine_result_ok_" + str(end_date).replace('-','') + "')"
	cursor.execute(sql_2)
	b = cursor.fetchall()[0][0]

	if a == 0 or b == 0:
		if a != 0:
			cursor.execute("drop table stock_mine_result_" +str(end_date).replace('-',''))
		if b != 0:
			cursor.execute("drop table stock_mine_result_ok_" + str(end_date).replace('-', ''))
		# 只要有一张表不存在，重新建两张表
		sql_3 = "create table stock_mine_result_" +str(end_date).replace('-','')+ \
 			" (uuid varchar2(100), stock_code_1 varchar2(20), stock_table_name_1 varchar2(20), \
 			stock_code_2 varchar2(20), stock_table_name_2 varchar2(20), MIC number(10,4), \
 			MAS number(10,4), MEV number(10,4), MCN number(10,4), \
 			GMIC number(10,4), TIC number(10,4), COV number(10,4), CORR number(10,4))"
		sql_4 = "create table stock_mine_result_ok_" + str(end_date).replace('-', '') + \
			"(ok_1 varchar2(30), ok_2 varchar2(30))"
		cursor.execute(sql_3)
		cursor.execute(sql_4)
		print('建表完毕')
	else:
		#不建表
		print('不用建表')
		pass

	cursor.close()



def ca_mic_corr(table_name_1, table_name_2, start_date, end_date):
	# time_1 = time.time()
	cursor = conn.getConfig()

	sql_1 = "select sdate, close " + \
			" from " + table_name_1 + \
			" where sdate between to_date('" + start_date + "', 'yyyy-MM-dd')  \
			and to_date('" + end_date + "', 'yyyy-MM-dd')"
	cursor.execute(sql_1)
	result_1 = pd.DataFrame(cursor.fetchall(), columns=['sdate', 'close_1'])
	sql_2 = "select sdate, close " + \
			" from " + table_name_2 + \
			" where sdate between to_date('" + start_date + "', 'yyyy-MM-dd')  \
			and to_date('" + end_date + "', 'yyyy-MM-dd')"
	cursor.execute(sql_2)
	result_2 = pd.DataFrame(cursor.fetchall(), columns=['sdate', 'close_2'])
	x = pd.merge(result_1, result_2, on='sdate', how='inner')
	mine = MINE(alpha=0.6, c=15, est="mic_approx")
	mine.compute_score(x['close_1'], x['close_2'])
	# 计算MIC各类指标
	mic, mas, mev, mcn, gmic, tic = return_stats(mine)

	dfab = pd.DataFrame(np.array([x['close_1'], x['close_2']]).T, columns=['A', 'B'])
	# 计算协方差及相关系数
	cov = round(float(dfab.A.cov(dfab.B)), 4)  # 协方差
	corr = round(float(dfab.A.corr(dfab.B)), 4)  # 相关系数

	if math.isnan(corr):
		pass
	else:
		cursor.execute("insert into stock_mine_result_" + str(end_date).replace('-','') + \
						"(uuid, stock_code_1, stock_table_name_1, stock_code_2, stock_table_name_2, \
						 mic, mas, mev, mcn, gmic, tic, cov, corr) \
						 values ('%s', '%s', '%s', '%s', '%s', \
						 '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f')" % (
			str(uuid.uuid1()),
			str(table_name_1).replace("STOCK_", ""),
			str(table_name_1),
			str(table_name_2).replace("STOCK_", ""),
			str(table_name_2),
			mic, mas, mev, mcn, gmic, tic, cov, corr
		))
		cursor.execute("commit")
		# time_2 = time.time()

		# print(table_name_1, table_name_2, "	OK", time_2 - time_1)
	cursor.close()


def return_stats(mine):
	# mic, mas, mev, mcn, gmic, tic
	return round(float(mine.mic()), 4), round(float(mine.mas()), 4), \
		round(float(mine.mev()), 4), round(float(mine.mcn(0)), 4), \
		round(float(mine.gmic()), 4), round(float(mine.tic()), 4)


if __name__ == '__main__':
	# 开始时间
	start_date = "2018-01-01"
	# 结束时间，即求取该时间范围内的数据
	end_date = '2019-02-22'
	res_list = get_stock_data(start_date, end_date)
	print(res_list)

	# 建表
	create_base_table(end_date)

	# 查是否已处理
	cursor = conn.getConfig()
	sql_1 = "select ok_1, ok_2 from stock_mine_result_ok_" + str(end_date).replace('-', '')
	cursor.execute(sql_1)
	ok_list = cursor.fetchall()
	ok_group_list = []
	for i in ok_list:
		ok_group_list.append(str(i[0]) + "_" + str(i[1]))

	print(ok_group_list)


	# 两两组合
	for i in itertools.combinations(res_list, r=2):
		# 计算相关性并入库
		if str(i[0])+"_"+str(i[1]) not in ok_group_list:
			ca_mic_corr(i[0], i[1], start_date, end_date)
			cursor.execute("insert into stock_mine_result_ok_" + str(end_date).replace('-', '') + \
				"(ok_1, ok_2) values('%s', '%s')" % (str(i[0]), str(i[1])))
			cursor.execute(sql_1)
			cursor.execute("commit")


