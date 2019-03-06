# -*- coding=utf-8 -*-
# 获取上证指数数据

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



# 通过网易财经获取全量数据的CSV文件
def getCSV(code, url):
	fordername = 'AllStockData\\'
	filename = str(code) + '.CSV'
	with request.urlopen(url) as web:
		# 为防止编码错误，使用二进制写文件模式
		with open(fordername+filename, 'wb') as outfile:
			outfile.write(web.read())

	saveInDB(code)


# 将获取的数据入库
def saveInDB(code):
	conn = MongoClient('localhost', 27017)
	my_db = conn.stock  # 连接stock数据库，没有则自动创建
	my_set = my_db.stock_sse

	# 先删后插
	x = my_set.delete_many({})
	print(x.deleted_count, "条记录已删除")


	# 解析CSV文件并数据清洗
	fordername = 'AllStockData\\'
	filename = str(code)+'.CSV'
	df = pd.read_csv(fordername + filename, encoding='gbk')

	df.rename(columns={u'日期': 'sdate', u'股票代码': 'code', u'名称': 'name', u'收盘价': 'close', u'最高价': 'high',
					   u'最低价': 'low', u'开盘价': 'open', u'前收盘': 'y_close', u'涨跌额': 'p_change', u'涨跌幅': 'p_change_rate',
					   u'换手率': 'turnover', u'成交量': 'volume', u'成交金额': 'amount', u'总市值': 'marketcap',
					   u'流通市值': 'famc', u'成交笔数': 'zbs'}, inplace=True)
	df['code'] = code
	df['classify'] = "指数"
	dfLen = len(df)
	df['uuid'] = [uuid.uuid1() for l in range(0, dfLen)]  # 添加uuid

	# 处理None
	df = df.replace('None', 0)

	# 转浮点数
	df['amount'] = [as_num(float(x)) for x in df['amount']]
	df['marketcap'] = 0
	df['famc'] = 0
	df['turnover'] = 0

	# 入库
	for k in range(0, dfLen):
		df2 = df[k:k + 1]
		# print(df2)
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
	print("插入完毕")


# 将科学记数法化为浮点数
def as_num(x):
	y = '{:.4f}'.format(x)  # 4f表示保留4位小数点的float型
	return(y)



def main():
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	code = "000001_SSE"
	url = "http://quotes.money.163.com/service/chddata.html?code=0000001&start=19901219&end="+systemTime
	getCSV(code, url)


if __name__ == '__main__':
	main()