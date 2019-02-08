# -*- coding: utf-8 -*-
# 计算收益率
# 计算波动率

import pandas as pd
import time
from multiprocessing import Pool
import uuid
import numpy as np
from datetime import datetime
# 导入连接文件
import sys
sys.path.append('..')
import common.GetOracleConn as conn


def get_stock_data(end_date):
	start_date = '2018-01-01'
	time_1 = time.time()
	cursor = conn.getConfig()
	sql_1 = "select table_name from user_tables \
			 where ((table_name like 'STOCK_6%' \
             or table_name like 'STOCK_0%' or table_name like 'STOCK_3') \
             and substr(table_name, 7, 10) in \
             (select symbol from stock_basics_2 where list_status='L') ) \
             or table_name = 'STOCK_000001_SSE' \
             or table_name = 'STOCK_399001_SZSE' "
	cursor.execute(sql_1)
	res = cursor.fetchall()
	res_list = []

	# 获取库存指数最大日期
	sql_2 = "select max(sdate) from stock_000001_sse"
	cursor.execute(sql_2)
	max_date = str(cursor.fetchall()[0][0])[0:10]
	if end_date > max_date:
		end_date = max_date
	else:
		end_date = end_date

	for i in res:
		res_list.append(i[0])
		sql_3 = "select sdate, close, y_close from " + str(i[0]) + \
				" where sdate >= to_date(\'" + start_date + "\', \'yyyy-MM-dd\') " + \
				" and sdate <= to_date(\'" + end_date + "\', \'yyyy-MM-dd\') " + \
				" and close > 0 "
		# print(sql_2)
		cursor.execute(sql_3)
		df = pd.DataFrame(cursor.fetchall(), columns=['sdate', 'close', 'y_close'])
		df = df.dropna()
		# 对数收盘价
		close_log = np.log(df['close'])
		df['close_log'] = close_log
		# 对数昨收盘
		y_close_log = np.log(df['y_close'])
		df['y_close_log'] = y_close_log
		# 对数收益率
		log_rate = close_log - y_close_log
		df['log_rate'] = log_rate
		# 波动率
		# 此处的波动率和下面的交易天数有关，为len(df)日的波动率
		volatility = np.std(log_rate)
		df['volatility'] = volatility
		# 交易天数
		df['trade_days'] = len(df)
		df['stock_table_name'] = i[0]

		# 保存结果
		# df.to_csv(forder_name + "\\" + i[0] + "_" + start_date + ".csv", index=False)
		df_stock_table_name = i[0]
		df_stock_name = i[0][6:]
		df_start_date = start_date  # 开始时间
		df_end_date = end_date  # 结束时间
		df_log_rate = round(float(str(np.sum(df['log_rate'])).replace('nan', '0')),4)  # 收益率
		df_vol = round(float(str(np.average(df['volatility'])).replace('nan', '0')),4)  # 波动率
		# print(df_stock_name)
		# 入库
		insert_db(cursor, df_stock_table_name, df_stock_name, df_start_date,
			  df_end_date, df_log_rate, df_vol)

		# print(df_stock_table_name, df_stock_name, df_start_date,
		#  	  df_end_date, df_log_rate, df_vol)
	cursor.close()
	time_2 = time.time()
	print(start_date, end_date, "	OK", time_2-time_1)


def insert_db(cursor, df_stock_table_name, df_stock_name, df_start_date,
			  df_end_date, df_log_rate, df_vol):
	# cursor = conn.getConfig()
	# print(df_stock_table_name, df_stock_name, df_start_date,
	# 	  df_end_date, df_log_rate, df_vol)
	cursor.execute("insert into stock_return_vol(uuid, stock_table_name, stock_code, start_date, \
             end_date, log_rate, vol) values('%s', '%s', '%s', to_date('%s', 'yyyy-MM-dd'), \
             to_date('%s', 'yyyy-MM-dd'), '%f', '%f')" % (
		uuid.uuid1(), df_stock_table_name, df_stock_name, df_start_date,
		df_end_date, df_log_rate, df_vol
	))
	cursor.execute("commit")

	# cursor.close()


# 生成时间序列
def datelist(beginDate, endDate):
	# beginDate, endDate是形如‘20160601’的字符串或datetime格式
	date_l = [datetime.strftime(x, '%Y-%m-%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
	return date_l


if __name__ == '__main__':
	cursor = conn.getConfig()
	end_date = '2019-01-30'
	sql_1 = "select max(end_date) from stock_return_vol"
	cursor.execute(sql_1)
	start_end_date = str(cursor.fetchall()[0][0])[0:10]
	if start_end_date == 'None':
		start_end_date='2018-01-01'

	cursor.close()

	end_date_list = datelist(start_end_date, end_date)

	# for end_date in end_date_list:
	# 	if start_end_date != end_date:
	# 		# print(end_date)
	# 		get_stock_data(end_date)
	# 		# print(start_date, end_date)

	pool = Pool(processes = 9)  # 设定并发进程的数量
	pool.map(get_stock_data, (end_date for end_date in end_date_list))

