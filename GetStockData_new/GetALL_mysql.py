# -*- coding=utf-8 -*-
# 获取全部股票信息

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
import uuid
import os
import numpy as np
import pymysql
from multiprocessing import Pool


# 连接数据库
def get_cursor():
	conn = pymysql.connect(host='127.0.0.1', user='root',
						   passwd='123456', db='stock', port=3306, charset='utf8')
	cursor = conn.cursor()
	return cursor


# 列出所有股票代码及名称，并存入dictionary
def listStock():
	cursor = get_cursor()
	dict1 = {}  #存放开头为6的股票代码及访问链接
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	cursor.execute("select symbol from stock.stock_basics where symbol like '6%'")
	pdata1 = cursor.fetchall()
	# print(pdata1)
	for i in pdata1:
		dict1[i[0]] = 'http://quotes.money.163.com/service/chddata.html?code=0' + str(i[0]) + \
					  '&start=19900101&end=' + systemTime

	dict2 = {}  # 存放开头不为6的股票代码及访问链接
	cursor.execute("select symbol from stock.stock_basics where symbol not like '6%'")
	pdata2 = cursor.fetchall()
	for i in pdata2:
		dict2[i[0]] = 'http://quotes.money.163.com/service/chddata.html?code=1' + str(i[0]) + \
					  '&start=19900101&end=' + systemTime

	# 合并两个字典
	dict3 = dict(dict1, **dict2)
	cursor.close()
	return dict3


# 通过网易财经获取全量数据的CSV文件
def getCSV(code, url):
	time_1 = time.time()
	fordername = 'AllStockData\\'
	filename = str(code) + '.CSV'
	with request.urlopen(url) as web:
		# 为防止编码错误，使用二进制写文件模式
		with open(fordername+filename, 'wb') as outfile:
			outfile.write(web.read())

	saveInDB(code)
	time_2 = time.time()
	print(code,	"存储完毕", time_2 - time_1)



# 将获取的数据入库
def saveInDB(code):
	# 建表
	create_table(code)

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

	# 入库
	cursor = get_cursor()
	for k in range(0, dfLen):
		try:
			df2 = df[k:k + 1]
			cursor.execute("insert into stock.stock_"+str(code)+""" values('%s', '%s', '%s', '%s', '%s', '%s', 
			'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""
			% (str(list(df2['uuid'])[0]), str(list(df2['sdate'])[0]),
			str(list(df2['code'])[0]), str(list(df2['name'])[0]),
			str(list(df2['classify'])[0]), round(float(df2['open']), 4),
			round(float(df2['close']), 4), round(float(df2['high']), 4), round(float(df2['low']), 4),
			round(float(df2['volume']), 4), round(float(df2['amount']), 4),
			round(float(df2['y_close']), 4), round(float(df2['p_change']), 4),
			round(float(df2['p_change_rate']), 4), round(float(df2['turnover']), 4),
			round(float(df2['marketcap']), 4), round(float(df2['famc']), 4),
			round(float(df2['zbs']), 4) ) )
		except Exception:
			pass
	cursor.execute("commit")
	cursor.close()


# 建表
def create_table(code):
	cursor = get_cursor()
	sql_1 = "drop table if exists stock.stock_" + code
	cursor.execute(sql_1)

	sql_2 = """CREATE TABLE stock.STOCK_""" + code + """ 
	(UUID VARCHAR(80), SDATE DATE, CODE VARCHAR(20), NAME VARCHAR(80), 
	CLASSIFY VARCHAR(80), OPEN float(20), CLOSE float(20), HIGH float(20), 
	LOW float(20), VOLUME float(20), AMOUNT float(20), Y_CLOSE float(20), 
	P_CHANGE float(20), P_CHANGE_RATE float(20), TURNOVER float(20), 
	MARKETCAP float(20), FAMC float(20), ZBS float(20))"""
	cursor.execute(sql_2)
	cursor.close()


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



# 单进程入库
def s_get_stock_csv():
	dict = listStock()
	print(dict)

	for key in dict:
		getCSV(key, dict[key])


# 多进程入库
def m_get_stock_csv():
	dict = listStock()
	pool = Pool(processes=2)  # 设定并发进程的数量
	for key in dict:
		pool.apply_async(getCSV, (key, dict[key],))

	pool.close()
	pool.join()



if __name__ == '__main__':
	s_get_stock_csv()
	# m_get_stock_csv()