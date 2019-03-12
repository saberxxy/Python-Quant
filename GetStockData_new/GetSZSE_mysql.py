# -*- coding=utf-8 -*-
# 获取深交所指数数据

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
import pymysql
import uuid
import os
import numpy as np
import tushare as ts


# 连接数据库
def get_cursor():
	conn = pymysql.connect(host='127.0.0.1', user='root',
						   passwd='123456', db='stock', port=3306, charset='utf8')
	cursor = conn.cursor()
	return cursor


# 获取深圳指数
def get_data(code, end_date):
	ts.set_token('570214b6505cdbe07edd464a73c6169b3033ceddbfce7efc12cade26')
	pro = ts.pro_api()
	df = pro.index_daily(ts_code=str(code)+'.SZ', start_date='19901219', end_date='20301231')
	return df


# 存入数据
def insert_data(df, code):
	# 建表
	create_table(code)

	df = df.replace('None', 0)

	df2 = pd.DataFrame()
	df2['uuid'] = [uuid.uuid1() for l in range(0, len(df))]  # 添加uuid
	df2['code'] = str(code)
	df2['sdate'] = df['trade_date']
	df2['close'] = df['close']
	df2['open'] = df['open']
	df2['high'] = df['high']
	df2['low'] = df['low']
	df2['y_close'] = df['pre_close']
	df2['p_change'] = df['change']
	df2['P_CHANGE_RATE'] = df['pct_chg']
	df2['VOLUME'] = df['vol']
	df2['amount'] = df['amount']

	cursor = get_cursor()

	# 入库
	for k in range(0, len(df2)):
		df3 = df2[k:k + 1]
		try:
			cursor.execute("insert into stock.stock_" + str(code) + \
			"""_SZSE(uuid, code, sdate, close, open, high,
			low, y_close, p_change, P_CHANGE_RATE, VOLUME, amount) 
			values('%s', '%s', '%s', '%s', '%s', '%s',
			'%s', '%s', '%s', '%s', '%s', '%s')""" % ( (str(list(df3['uuid'])[0]),
							 str(list(df3['code'])[0]),
							 str(list(df3['sdate'])[0]),
							 str(list(df3['close'])[0]),
							 str(list(df3['open'])[0]),
							 str(list(df3['high'])[0]),
							 str(list(df3['low'])[0]),
							 str(list(df3['y_close'])[0]),
							 str(list(df3['p_change'])[0]),
							 str(list(df3['P_CHANGE_RATE'])[0]),
							 str(list(df3['VOLUME'])[0]),
							 str(list(df3['amount'])[0])) ) )
		except Exception:
			pass
	cursor.execute("commit")
	print("插入成功")
	cursor.close()



# 建表
def create_table(code):
	cursor = get_cursor()

	sql_1 = "drop table if exists stock.STOCK_" + code + "_SZSE"
	cursor.execute(sql_1)

	sql_2 = "CREATE TABLE stock.STOCK_" + code + "_SZSE" + \
	"""(uuid varchar(80), code varchar(20), sdate date, close float(20),
		open float(20), high float(20), low float(20), y_close float(20), 
		p_change float(20), P_CHANGE_RATE float(20), VOLUME float(20),
		amount float(20))"""
	cursor.execute(sql_2)

	cursor.close()
	print('建表成功')



def main():
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	code = "399001"
	df = get_data(code, systemTime)
	print(df.head())
	insert_data(df, code)



if __name__ == '__main__':
	main()