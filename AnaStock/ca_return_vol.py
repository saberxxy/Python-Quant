# -*- coding: utf-8 -*-
# 计算收益率
# 计算波动率

import pandas as pd
import time
from multiprocessing import Pool
import uuid
import os
import numpy as np
import re
import matplotlib.pyplot as plt
# 导入连接文件
import sys
sys.path.append('..')
import common.GetOracleConn as conn


def get_stock_data(start_date, forder_name):
	cursor = conn.getConfig()
	sql_1 = "select table_name from user_tables where (table_name like 'STOCK_6%' \
          or table_name like 'STOCK_0%' or table_name like 'STOCK_3') \
          and table_name <> 'STOCK_000001_SSE' and substr(table_name, 7, 10) in \
          (select symbol from stock_basics_2 where list_status='L') "
	cursor.execute(sql_1)
	res = cursor.fetchall()
	res_list = []
	for i in res:
		time_1 = time.time()
		res_list.append(i[0])
		sql_2 = "select sdate, close, y_close from " + str(i[0]) + \
				" where sdate >= to_date(\'" + start_date + "\', \'yyyy-MM-dd\') " + \
				" and close > 0 "
		# print(sql_2)
		cursor.execute(sql_2)
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
		df.to_csv(forder_name + "\\" + i[0] + ".csv", index=False)
		time_2 = time.time()
		print(i[0], "	OK", time_2-time_1)


if __name__ == '__main__':
	start_date = '2018-01-01'
	forder_name = "F:\\Program\\Python\\Quant\\AnaStock\\return_vol_results_" + \
		start_date.replace('-', '')
	os.makedirs(forder_name, exist_ok=True)
	get_stock_data(start_date, forder_name)

