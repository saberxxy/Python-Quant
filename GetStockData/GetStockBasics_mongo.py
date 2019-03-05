# -*- coding=utf-8 -*-
# 获取上市公司信息
# 获取上市公司信息，更新新接口的信息

import tushare as ts
import uuid
from pymongo import MongoClient


def main():
	conn = MongoClient('localhost', 27017)
	my_db = conn.stock  # 连接stock数据库，没有则自动创建
	my_set = my_db.stock_basics

	# 先删后插
	x = my_set.delete_many({})
	print(x.deleted_count, "个文档已删除")

	pro = ts.pro_api()
	df = pro.stock_basic(fields='ts_code, symbol, name, area, industry, fullname, \
	                                  enname, market, exchange, curr_type, list_status, \
	                                  list_date, delist_date, is_hs')
	df = df.replace('None', 0)

	ts_code = list(df['ts_code'])
	symbol = list(df['symbol'])
	name = list(df['name'])
	area = list(df['area'])
	industry = list(df['industry'])
	fullname = list(df['fullname'])
	enname = list(df['enname'])
	market = list(df['market'])
	exchange = list(df['exchange'])
	curr_type = list(df['curr_type'])
	list_status = list(df['list_status'])
	list_date = list(df['list_date'])
	delist_date = list(df['delist_date'])
	is_hs = list(df['is_hs'])

	dfLen = len(df)
	for i in range(0, dfLen):
		ts_code_db = str(ts_code[i])
		symbol_db = str(symbol[i])
		name_db = str(name[i])
		area_db = str(area[i])
		industry_db = str(industry[i])
		fullname_db = str(fullname[i])
		enname_db = str(enname[i]).replace("'", "")
		market_db = str(market[i])
		exchange_db = str(exchange[i])
		curr_type_db = str(curr_type[i])
		list_status_db = str(list_status[i])
		list_date_db = str(list_date[i])
		delist_date_db = str(delist_date[i])
		is_hs_db = str(is_hs[i])

		my_set.insert({"uuid": str(uuid.uuid1()),
				   "ts_code": ts_code_db,
				   "symbol": symbol_db,
				   "name": name_db,
				   "area": area_db,
				   "industry": industry_db,
				   "fullname": fullname_db,
				   "enname": enname_db,
				   "market": market_db,
				   "exchange": exchange_db,
				   "curr_type": curr_type_db,
				   "list_status": list_status_db,
				   "list_date": list_date_db,
				   "delist_date": delist_date_db,
				   "is_hs": is_hs_db } )
	print('插入成功')



if __name__ == '__main__':
	main()
