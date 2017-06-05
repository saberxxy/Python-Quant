#-*- coding=utf-8 -*-
#SVM支持向量机测试

import sys

import pymysql
from sklearn import svm
import time
import numpy as np

mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword, db=mysqlDatabaseName, port=mysqlPort, charset="utf8")

def indexOneSVM():
    #表查询语句
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv = []  #特征，训练数据
    for res in result1:
        a = []
        a.append(float(list(res)[1]))  #开盘价
        a.append(float(list(res)[2]))  #最高价
        a.append(float(list(res)[3]))  #收盘价
        a.append(float(list(res)[4]))  #最低价
        a.append(int(list(res)[5]))   #成交量
        a.append(int(list(res)[6]))   #成交金额
        a.append(float(list(res)[7]))  #前收盘
        fv.append(a)

    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla = []  #分类数据，0为跌，1为涨
    for res in result2:
        cla.append(int(list(res)[0]))

    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test = []  #测试数据，对应上述训练数据的结构
    for res in result3:
        test.append(float(list(res)[1]))
        test.append(float(list(res)[2]))
        test.append(float(list(res)[3]))
        test.append(float(list(res)[4]))
        test.append(int(list(res)[5]))
        test.append(int(list(res)[6]))
        test.append(float(list(res)[7]))

    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    clf = svm.SVC()  # 支持向量机
    clf.fit(fv, cla)
    test = [test]  # 转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction

#指标组二
def indexTwoSVM():
    #表查询语句
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv = []  #特征
    for res in result1:
        a = []
        a.append(float(list(res)[1]))
        a.append(float(list(res)[2]))
        a.append(float(list(res)[3]))
        a.append(float(list(res)[4]))
        fv.append(a)

    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla = []  #分类
    for res in result2:
        cla.append(int(list(res)[0]))

    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test = []  #测试数据
    for res in result3:
        test.append(float(list(res)[1]))
        test.append(float(list(res)[2]))
        test.append(float(list(res)[3]))
        test.append(float(list(res)[4]))

    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    clf = svm.SVC()  # 支持向量机
    clf.fit(fv, cla)
    test = [test]  # 转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction


#指标组三
def indexThreeSVM():
    #表查询语句
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv = []  #特征
    for res in result1:
        a = []
        a.append(float(list(res)[3]))
        a.append(float(list(res)[5]))
        fv.append(a)

    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla = []  #分类
    for res in result2:
        cla.append(int(list(res)[0]))

    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test = []  #测试数据
    for res in result3:
        test.append(float(list(res)[3]))
        test.append(float(list(res)[5]))

    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    clf = svm.SVC()  # 支持向量机
    clf.fit(fv, cla)
    test = [test]  # 转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction


def main():
    time1 = time.time()

    test1, pre1 = indexOneSVM()
    print (u"支持向量机指标组一：", test1, pre1)
    test2, pre2 = indexTwoSVM()
    print (u"支持向量机指标组二：", test2, pre2)
    test3, pre3 = indexThreeSVM()
    print (u"支持向量机指标组三：", test3, pre3)

    time2 = time.time()
    print ((time2 - time1)/60, u"分钟")



if __name__ == '__main__':
    main()


