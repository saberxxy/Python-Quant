# -*- coding=utf-8 -*-
# 获取深交所指数数据

import urllib
import urllib.request
from urllib import request
import uuid
import pandas as pd
import time
from multiprocessing import Pool
import uuid
import os
import numpy as np
import cProfile
import tushare as ts


# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn

# 获取全局数据库连接
cursor = conn.getConfig()


# 获取深圳指数
def get_data(code, end_date):
	ts.set_token('afd51ad37bf91b4c98f871fa676d4f67fcd92c8bcbfb12fad231f831')
	pro = ts.pro_api()
	df = pro.index_daily(ts_code=str(code)+'.SZ', start_date='19901219', end_date='20181031')
	return df


# 存入数据
def insert_data(df, code):
	# 建表
	if not is_table_exist(code):  # 如果表不存在，先创建表
		create_table(code)  # 如果表不存在，先建表
	else:  # 存在则截断
		cursor.execute("truncate table stock_" + code + "_SZSE")
		print('表已清空')

	print(df.head())
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
		sql = "insert into stock_" + str(code) + "_SZSE(uuid, code, sdate, close, open, high,  \
			low, y_close, p_change, P_CHANGE_RATE, VOLUME, amount) \
			values(:uuid, :code, to_date(:sdate, 'yyyy-MM-dd'), :close, :open, :high, \
			:low, :y_close, :p_change, :P_CHANGE_RATE, :VOLUME, :amount)"

		cursor.execute(sql, (str(list(df3['uuid'])[0]),
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
							 str(list(df3['amount'])[0])))
		cursor.execute("commit")
		print("插入成功")


# 判断表是否已存在
def is_table_exist(code):
	table = "STOCK_" + code + "_SZSE"
	sql = "select table_name from user_tables"
	rs = cursor.execute(sql)
	result = rs.fetchall()
	tables = [i[0] for i in result]
	print(table)
	print(tables.__contains__(table))
	return tables.__contains__(table)


# 建表
def create_table(code):
	sql = "CREATE TABLE STOCK_" + code + "_SZSE" + \
	"""(uuid varchar2(80) primary key,
		code varchar2(20),
		sdate date,
		close number(20, 4),
		open number(20, 4),
		high number(20, 4),
		low number(20, 4),
		y_close number(20, 4),
		p_change number(20, 4),
		P_CHANGE_RATE number(20, 4),
		VOLUME number(20, 4),
		amount number(20, 4))"""
	cursor.execute(sql)
	# 添加注释
	comments = ["COMMENT ON TABLE STOCK_" + code + "_SZSE IS '深证指数'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.UUID IS 'UUID'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.SDATE IS '日期'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.CODE IS '股票代码'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.OPEN IS '开盘价'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.CLOSE IS '收盘价'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.HIGH IS '最高价'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.LOW IS '最低价'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.VOLUME IS '成交量'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.AMOUNT IS '成交金额'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.Y_CLOSE IS '昨收盘'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.P_CHANGE IS '涨跌额'",
		"COMMENT ON COLUMN STOCK_" + code + "_SZSE.P_CHANGE_RATE IS '涨跌幅'"]
	for i in comments:
		cursor.execute(i)

	print('建表成功')



def main():
	systemTime = str(time.strftime('%Y%m%d', time.localtime(time.time())))
	code = "399001"
	df = get_data(code, systemTime)
	print(df.head())
	insert_data(df, code)



if __name__ == '__main__':
	main()