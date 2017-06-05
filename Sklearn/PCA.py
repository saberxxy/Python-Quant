#-*- coding=utf-8 -*-
#PCA降维分析Principal Component Analysis

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from sklearn.decomposition import PCA
import MySQLdb

mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = MySQLdb.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword, db=mysqlDatabaseName, port=mysqlPort, charset="utf8")

cur1 = conn.cursor()
cur1.execute('select * from szzs_black_swan;')
result1 = cur1.fetchall()
fv = []  #特征，训练数据
cla = []  #分类
for res in result1:
    a = []
    b = []
    a.append(float(list(res)[1]))  #开盘价
    a.append(float(list(res)[2]))  #最高价
    a.append(float(list(res)[3]))  #收盘价
    a.append(float(list(res)[4]))  #最低价
    a.append(long(list(res)[5]))   #成交量
    a.append(long(list(res)[6]))   #成交金额
    a.append(float(list(res)[7]))  #前收盘
    a.append(float(list(res)[10]))  #涨跌幅
    fv.append(a)

    b.append(str(list(res)[0]))
    b.append(int(list(res)[9]))
    b.append(float(list(res)[11]))
    b.append(int(list(res)[13]))
    cla.append(b)

pca = PCA(n_components=6, copy=True)  #降为六维
"""
n_components:
意义：PCA算法中所要保留的主成分个数n，也即保留下来的特征个数n
类型：int 或者 string，缺省时默认为None，所有成分被保留。
赋值为int，比如n_components=1，将把原始数据降到一个维度。
赋值为string，比如n_components='mle'，将自动选取特征个数n，使得满足所要求的方差百分比。
"""
fv = pca.fit_transform(fv)

#降维后转为list，存储进d
d = []
for i in fv:
    c = []
    c.append(float(list(i)[0]))
    c.append(float(list(i)[1]))
    c.append(float(list(i)[2]))
    c.append(float(list(i)[3]))
    c.append(float(list(i)[4]))
    c.append(float(list(i)[5]))
    d.append(c)

for i in range(0,len(cla)):
    cla[i][1:1] = d[i]

# print cla
cur4 = conn.cursor()
cur4.execute("DROP TABLE IF EXISTS szzs_pca;")
sqlCreate = """create table szzs_pca
                (date varchar(100) comment '交易日期',
                pca1 varchar(100) comment '降维后指标一',
                pca2 varchar(100) comment '降维后指标二',
                pca3 varchar(100) comment '降维后指标三',
                pca4 varchar(100) comment '降维后指标四',
                pca5 varchar(100) comment '降维后指标五',
                pca6 varchar(100) comment '降维后指标六',
                rise_fall_next varchar(100) comment '明日涨跌',
                rise_fall_rate_next varchar(100) comment '明日涨跌幅',
                black_swan_next varchar(100) comment '明日是否是黑天鹅，0白天鹅，1灰天鹅，2黑天鹅'
                )"""
cur4.execute(sqlCreate)

for j in cla:
    date = str(j[0])
    pca1 = str(j[1])
    pca2 = str(j[2])
    pca3 = str(j[3])
    pca4 = str(j[4])
    pca5 = str(j[5])
    pca6 = str(j[6])
    riseFallNext = str(j[7])
    riseFallRateNext = str(j[8])
    blackSwanNext = str(j[9])

    cur4.execute(
    "INSERT INTO szzs_pca(DATE, PCA1, PCA2, PCA3, PCA4, PCA5, PCA6, RISE_FALL_NEXT, RISE_FALL_RATE_NEXT,BLACK_SWAN_NEXT) "
    "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"
    % (date, pca1, pca2, pca3, pca4, pca5, pca6, riseFallNext, riseFallRateNext, blackSwanNext))

cur4.execute("commit;")




