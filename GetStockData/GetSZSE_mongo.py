# -*- coding=utf-8 -*-
# 获取深交所指数数据

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
import uuid
import os
import numpy as np
import tushare as ts
from pymongo import MongoClient


# 获取深圳指数
def get_data(code, end_date):
	ts.set_token('570214b6505cdbe07edd464a73c6169b3033ceddbfce7efc12cade26')
	pro = ts.pro_api()
	df = pro.index_daily(ts_code=str(code)+'.SZ', start_date='19901219', end_date='20301231')
	return df


# 存入数据
def insert_data(df, code):
	conn = MongoClient('localhost', 27017)
	my_db = conn.stock  # 连接stock数据库，没有则自动创建
	my_set = my_db.stock_szse

	# 先删后插
	x = my_set.delete_many({})
	print(x.deleted_count, "条记录已删除")

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

	# 入库
	for k in range(0, len(df2)):
		df3 = df2[k:k + 1]

		my_set.insert({"uuid": str(list(df3['uuid'])[0]),
			"code": str(list(df3['code'])[0]),
			"sdate": str(list(df3['sdate'])[0])[0:4] + "-" + str(list(df3['sdate'])[0])[4:6] + "-" + str(list(df3['sdate'])[0])[6:8],
			"close": str(list(df3['close'])[0]),
			"open": str(list(df3['open'])[0]),
			"high": str(list(df3['high'])[0]),
			"low": str(list(df3['low'])[0]),
			"y_close": str(list(df3['y_close'])[0]),
			"p_change": str(list(df3['p_change'])[0]),
			"p_change_rate": str(list(df3['P_CHANGE_RATE'])[0]),
			"volume": str(list(df3['VOLUME'])[0]),
			"amount": str(list(df3['amount'])[0]) } )
	print("插入成功")


def main():
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	code = "399001"
	df = get_data(code, systemTime)
	insert_data(df, code)


if __name__ == '__main__':
	main()