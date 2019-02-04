# -*- coding=utf-8 -*-
# 导入用R跑出来的MIC信息

import tushare as ts
import cx_Oracle as cxo
import configparser
import uuid
import os
import pandas as pd
import time

# 导入连接文件
import sys
sys.path.append("..")
import common.GetOracleConn as conn


# cursor = conn.getConfig()
# cursor.close()

# 获取文件列表
def getfile_list(path):
	ret = []
	for root, dirs, files in os.walk(path):
		for filespath in files:
			ret.append(os.path.join(root, filespath))
	return ret


def get_mic_result_csv():
	time_1 = time.time()

	cursor = conn.getConfig()
	forder_name = "F:\\Program\\R\\Quant\\stock_analysis\\mic_result"
	ret = getfile_list(forder_name)
	for i in ret:
		df = pd.read_csv(i, names=['uuid', 'stock_code_1', 'stock_table_name_1', 'stock_code_2',
                                  'stock_table_name_2', 'mic', 'mas', 'mev', 'mcn', 'mic_r2',
								  'gmic', 'tic'])
		df['stock_code_1'] = df['stock_table_name_1']
		df['stock_code_2'] = df['stock_table_name_2']
		df = df.replace('None', 0)
		dfLen = len(df)
		for j in range(0, dfLen):
			cursor.execute("insert into stock_mine_result_20190201( \
				uuid, stock_code_1, stock_table_name_1, stock_code_2, stock_table_name_2, \
              	mic, mas, mev, mcn, mic_r2, gmic, tic) \
			  	values ( \
			  	'%s', '%s', '%s', '%s', '%s', '%f', '%f', '%f', '%f', '%f', '%f', '%f')" % (
			  	str(df.iloc[j,]['uuid']),
			  	str(df.iloc[j, ]['stock_code_1']).replace("STOCK_", ""),
			  	str(df.iloc[j, ]['stock_table_name_1']),
			  	str(df.iloc[j, ]['stock_code_2']).replace("STOCK_", ""),
			  	str(df.iloc[j, ]['stock_table_name_2']),
			  	round(float(str(df.iloc[j, ]['mic']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['mas']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['mev']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['mcn']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['mic_r2']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['gmic']).replace("nan", "0")), 4),
			  	round(float(str(df.iloc[j, ]['tic']).replace("nan", "0")), 4)
			))
		cursor.execute("commit")
		time_2 = time.time()
		print(i, "	OK", time_2-time_1)

	cursor.close()


if __name__ == '__main__':
	get_mic_result_csv()