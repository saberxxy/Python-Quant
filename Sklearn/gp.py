# -*- coding:utf-8 -*-
import tushare as ts
from pandas import Series, DataFrame
import pandas as pd
import uuid
import re

# 1228    上海临港
# Name: name, dtype: object
# 1228上海临港Name:name,dtype:object

# 通过股票分类方法获得股票代码与中文名称的对应关系
all_gp = ts.get_industry_classified()

#获取历史数据
def getData(code, start=None, end=None):
    data = ts.get_k_data(code=code, start=start, end=end)
    data.code = code # 增加股票代号
    current_gp = all_gp[all_gp.code==code]
    gp_name = re.findall(r"\d+(.*)name", str(current_gp['name']).replace(" ", "").replace("\nName:name,dtype:object", "name"))
    data['name'] = gp_name[0] # 这里，为何不能用. 只能用[]
    # print(data['name'])
    return data

# 添加uuid字符串
def addColumns(data):
    data = DataFrame(data, columns=['uuid', 'date', 'code', 'name', 'open', 'close', 'high', 'low', 'volume'])
    for i in data.index:
        data.loc[i, ['uuid']] = str(uuid.uuid1()) + "_" + data.loc[i].date
    # print(data)
    return data


def main():
    data = getData(code=None)
    data = addColumns(data)


if __name__ == '__main__':
    main()

# #保存csv方法
# def save(data,filename):
# 	data.to_csv('E:/tushare/'+filename+'.csv')
#
# save(data, data.name)


# 获取hdfs连接
# client = Client('http://hadoop01:50070')
#
#写文件
# with client.write(hdfs_path="/tushare/1.txt", data="hello") as fs:
#     fs.write()


# 上传csv文件
# def uplode(hdfsFile, localFile):
#     client.upload(hdfs_path='/tushare/'+hdfsFile, local_path='E:/tushare/'+localFile)

# uplode(data.name, data.name+".csv")

# 获取oracle连接
# conn = cx_Oracle.connect("ys/123456@localhost/orcl")
# cursor = conn.cursor()
# cursor.execute("create table gp_"+ data.name +"(id number(*,0), data date, open number(2,2), close number(2,2), \
# high number(2,2), low number(2,2), volume number(*,0), code varchar2(6))")
# cursor.close()
# conn.close()
