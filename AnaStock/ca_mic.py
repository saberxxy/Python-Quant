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


def get_stock_data(date_time):
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
				" where sdate >= to_date('" + date_time + "', 'yyyy-MM-dd') "
		cursor.execute(sql_2)
		if cursor.fetchall()[0][0] > 250:
			res_list.append(i[0])

	cursor.close()
	return res_list


def ca_mic(date_time, res_list):
	cursor = conn.getConfig()
	for i in res_list:
		sql_0 = "select * from stock_mine_result_20190201_ok"
		cursor.execute(sql_0)
		a = cursor.fetchall()
		ok_stock = []
		for j in a:
			ok_stock.append(j[0])

		time_1 = time.time()
		if i not in ok_stock:
			for j in res_list:
				if i != j:
					# print(i, j)
					sql_1 = "select sdate, close " + \
							" from " + i + \
							" where sdate >= to_date('" + date_time + "', 'yyyy-MM-dd') "
					cursor.execute(sql_1)
					result_1 = pd.DataFrame(cursor.fetchall(), columns=['sdate', 'close_1'])
					sql_2 = "select sdate, close " + \
							" from " + j + \
							" where sdate >= to_date('" + date_time + "', 'yyyy-MM-dd') "
					cursor.execute(sql_2)
					result_2 = pd.DataFrame(cursor.fetchall(), columns=['sdate', 'close_2'])
					x = pd.merge(result_1, result_2, on='sdate', how='inner')
					mine = MINE(alpha=0.6, c=15, est="mic_approx")
					mine.compute_score(x['close_1'], x['close_2'])
					mic, mas, mev, mcn, gmic, tic = return_stats(mine)
					cursor.execute("insert into stock_mine_result_20190201( \
									uuid, stock_code_1, stock_table_name_1, stock_code_2, stock_table_name_2, \
									mic, mas, mev, mcn, gmic, tic) \
									values ( \
									'%s', '%s', '%s', '%s', '%s', '%f', '%f', '%f', '%f', '%f', '%f')" % (
						str(uuid.uuid1()),
						str(i).replace("STOCK_", ""),
						str(i),
						str(j).replace("STOCK_", ""),
						str(j),
						round(float(mic), 4),
						round(float(mas), 4),
						round(float(mev), 4),
						round(float(mcn), 4),
						round(float(gmic), 4),
						round(float(tic), 4)
					))
			# cursor.execute("commit")
			cursor.execute("insert into stock_mine_result_20190201_ok values('%s')" % (i))
			cursor.execute("commit")
			time_2 = time.time()
			print(i, "	OK", time_2 - time_1)

	cursor.close()


def return_stats(mine):
	# mic, mas, mev, mcn, gmic, tic
	return mine.mic(), mine.mas(), mine.mev(), \
		mine.mcn(0), mine.gmic(), mine.tic()



if __name__ == '__main__':
	date_time = "2018-01-01"
	res_list = get_stock_data(date_time)
	ca_mic(date_time, res_list)

	# mine = MINE(alpha=0.6, c=15, est="mic_approx")
	# mine.compute_score([1,2,3], [1,2,3])
	# print_stats(mine)

