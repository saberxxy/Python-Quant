# -*- coding=utf-8 -*-
# 获取全量股票历史记录

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
import uuid
import os
import numpy as np
from pymongo import MongoClient
from multiprocessing import Pool


# 列出所有股票代码及名称，并存入dictionary
def listStock():
	dict1 = {}  #存放开头为6的股票代码及访问链接
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	# cursor.execute("select code from stock_basics where code like '6%'")
	# pdata1 = cursor.fetchall()

	conn = MongoClient('localhost', 27017)
	my_db = conn.stock  # 连接stock数据库，没有则自动创建
	my_set = my_db.stock_basics

	# 第一个参数为查询条件，空代表查询所有，第二个参数为要输出的字段
	for x in my_set.find({"symbol": {"$regex": "^6"}}, {"symbol"}):
		dict1[x['symbol']] = 'http://quotes.money.163.com/service/chddata.html?code=0' + str(x['symbol']) + \
					  '&start=19900101&end=' + systemTime

	dict2 = {}  # 存放开头不为6的股票代码及访问链接

	for y in my_set.find({"symbol": {"$regex": "^[^6]"}}, {"symbol"}):
		dict2[y['symbol']] = 'http://quotes.money.163.com/service/chddata.html?code=1' + str(y['symbol']) + \
					  '&start=19900101&end=' + systemTime

	# 合并两个字典
	dict3 = dict(dict1, **dict2)

	conn.close()
	return dict3


# 解析入库
def save_db(code):
	time_1 = time.time()
	# 解析CSV文件并数据清洗
	fordername = 'AllStockData\\'
	filename = str(code) + '.CSV'
	df = pd.read_csv(fordername + filename, encoding='gbk')
	df.rename(columns={u'日期': 'sdate', u'股票代码': 'code', u'名称': 'name', u'收盘价': 'close', u'最高价': 'high',
					   u'最低价': 'low', u'开盘价': 'open', u'前收盘': 'y_close', u'涨跌额': 'p_change', u'涨跌幅': 'p_change_rate',
					   u'换手率': 'turnover', u'成交量': 'volume', u'成交金额': 'amount', u'总市值': 'marketcap',
					   u'流通市值': 'famc', u'成交笔数': 'zbs'}, inplace=True)
	df['code'] = code
	df['classify'] = get_type(code)
	dfLen = len(df)
	df['uuid'] = [uuid.uuid1() for l in range(0, dfLen)]  # 添加uuid

	# 处理None
	df = df.replace('None', 0)

	# 转浮点数
	df['amount'] = [as_num(x) for x in df['amount']]
	df['marketcap'] = [as_num(y) for y in df['marketcap']]
	df['famc'] = [as_num(z) for z in df['famc']]

	# 连接数据库
	conn = MongoClient('localhost', 27017)
	my_db = conn.stock  # 连接stock数据库，没有则自动创建
	stock_table_name = "stock_" + code
	my_set = my_db[stock_table_name]

	# 先删后插
	x = my_set.delete_many({})
	# print(x.deleted_count, "条记录已删除")

	for k in range(0, dfLen):
		df2 = df[k:k + 1]
		my_set.insert({"uuid": str(list(df2['uuid'])[0]),
					   "sdate": str(list(df2['sdate'])[0]),
					   "code": str(list(df2['code'])[0]),
					   "name": str(list(df2['name'])[0]),
					   "classify": str(list(df2['classify'])[0]),
					   "open": round(float(df2['open']), 4),
					   "close": round(float(df2['close']), 4),
					   "high": round(float(df2['high']), 4),
					   "low": round(float(df2['low']), 4),
					   "volume": round(float(df2['volume']), 4),
					   "amount": round(float(df2['amount']), 4),
					   "y_close": round(float(df2['y_close']), 4),
					   "p_change": round(float(df2['p_change']), 4),
					   "p_change_rate": round(float(df2['p_change_rate']), 4),
					   "turnover": round(float(df2['turnover']), 4),
					   "marketcap": round(float(df2['marketcap']), 4),
					   "famc": round(float(df2['famc']), 4),
					   "zbs": round(float(df2['zbs']), 4) } )
	time_2 = time.time()
	conn.close()
	print(code, "	插入完毕", time_2-time_1)


# 获取股票分类信息
def get_type(code):
	code_pre = code[0:3]
	switcher = {
		'300': "创业板",
		'600': "沪市A股",
		'601': "沪市A股",
		'602': "沪市A股",
		'900': "沪市B股",
		'000': "深市A股",
		'002': "中小板",
		'200': "深市B股"
	}
	return switcher.get(code_pre, '未知')


# 将科学记数法化为浮点数
def as_num(x):
	y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
	return(y)


# 通过网易财经获取全量数据的CSV文件
def getCSV(code, url):
	time_1 = time.time()
	fordername = 'AllStockData\\'
	filename = str(code) + '.CSV'
	with request.urlopen(url) as web:
		# 为防止编码错误，使用二进制写文件模式
		with open(fordername+filename, 'wb') as outfile:
			outfile.write(web.read())

	time_2 = time.time()
	print(code, "	获取完毕", time_2-time_1)

	# save_db(code)
	# print(code)


# 单进程获取股票信息
def s_get_csv():
	dict = listStock()
	print(dict)

	# 空出获取数据的函数
	for key in dict:
		getCSV(key, dict[key])


# 单进程入库
def s_import_data():
	dict = listStock()
	print(dict)

	# 空出获取数据的函数
	for key in dict:
		save_db(key)


# 多进程获取股票信息
def m_get_csv():
	dict = listStock()
	pool = Pool(processes = 3)  # 设定并发进程的数量
	for key in dict:
		pool.apply_async(getCSV, (key, dict[key],))

	pool.close()
	pool.join()


# 多进程入库
def m_imprt_data():
	dict = listStock()
	pool = Pool(processes=3)  # 设定并发进程的数量
	for key in dict:
		pool.apply_async(save_db, (key,))

	pool.close()
	pool.join()





if __name__ == '__main__':
	m_get_csv()
	# m_imprt_data()

	# s_get_csv()
	# s_import_data()


