#-*- coding=utf-8 -*-
#Logistic回归

import pymysql
import time
from sklearn.linear_model import LogisticRegression
import numpy as np

mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword, db=mysqlDatabaseName, port=mysqlPort, charset="utf8")

#指标组一
def indexOneLR():
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
        a.append(int(list(res)[5]))
        a.append(int(list(res)[6]))
        a.append(float(list(res)[7]))
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
        test.append(int(list(res)[5]))
        test.append(int(list(res)[6]))
        test.append(float(list(res)[7]))

    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    clf = LogisticRegression(C=1000.0, random_state=0, solver='liblinear')
    clf.fit(fv, cla)
    test = [test]  # 转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction

#指标组二
def indexTwoLR():
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

    clf = LogisticRegression(C=1000.0, random_state=0, solver='liblinear')
    clf.fit(fv, cla)
    test = [test]  #转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction

#指标组三
def indexThreeLR():
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

    clf = LogisticRegression(C=1000.0, random_state=0, solver='liblinear')
    clf.fit(fv, cla)
    test = [test]  #转为二维数组符合新写法
    prediction = clf.predict(test)

    return test, prediction

def main():
    time1 = time.time()

    test1, pre1 = indexOneLR()
    print (u"Logistic回归指标组一：", test1, pre1)
    test2, pre2 = indexTwoLR()
    print (u"Logistic回归指标组二：", test2, pre2)
    test3, pre3 = indexThreeLR()
    print (u"Logistic回归指标组三：", test3, pre3)

    time2 = time.time()
    print ((time2 - time1)/60, u"分钟")



if __name__ == '__main__':
    main()






#
#
# fv = [[1,1,1,1],
#       [2,2,2,2],
#       [3,3,3,3],
#       [4,4,4,4]]
# cla = [1,2,3,4]
# clf.fit(fv, cla)
# prediction = clf.predict([4,4,4,4])
#
# print(prediction)