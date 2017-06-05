#-*- coding=utf-8 -*-
#kNN邻近测试

import sys
import pymysql
from sklearn import neighbors
import time
import numpy as np


mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword, db=mysqlDatabaseName, port=mysqlPort, charset="utf8")

def indexOneKNN():
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

    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
        algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None, n_jobs=1) #取得knn分类器
    """参数讲解
    n_neighbors 就是 kNN 里的 k。
    weights 是在进行分类判断时给最近邻附上的加权，默认的 'uniform' 是等权加权，还有 'distance' 选项是按照距离的倒数进行加权。
    algorithm 是分类时采取的算法，有 'brute'、'kd_tree' 和 'ball_tree'。
    leaf_size 是 kd_tree 或 ball_tree 生成的树的树叶（树叶就是二叉树中没有分枝的节点）的大小。
    metric 和 p，是我们在 kNN 入门文章中介绍过的距离函数的选项，如果 metric ='minkowski' 并且 p=p 的话，计算两点之间的距离就是
    d((x1,…,xn),(y1,…,yn))=(∑i=1n|xi−yi|p)1/p
    d((x1,…,xn),(y1,…,yn))=(∑i=1n|xi−yi|p)1/p
    一般来讲，默认的 metric='minkowski'（默认）和 p=2（默认）就可以满足大部分需求。其他的 metric 选项可见说明文档。
    n_jobs 是并行计算的线程数量，默认是 1，输入 -1 则设为 CPU 的内核数。

    """
    knn.fit(fv, cla) #导入数据进行训练
    test = [test]  # 转为二维数组符合新写法
    prediction = knn.predict(test)
    return test, prediction

def indexOneKNN():
    x = 10 ** 7
    y = 10 ** 8
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
        a.append(int(list(res)[5])/x)   #成交量，归一化
        a.append(int(list(res)[6])/y)   #成交金额，归一化
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
        test.append(int(list(res)[5])/x)  #归一化
        test.append(int(list(res)[6])/y)  #归一化
        test.append(float(list(res)[7]))


    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
        algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None, n_jobs=1) #取得knn分类器
    """参数讲解
    n_neighbors 就是 kNN 里的 k。
    weights 是在进行分类判断时给最近邻附上的加权，默认的 'uniform' 是等权加权，还有 'distance' 选项是按照距离的倒数进行加权。
    algorithm 是分类时采取的算法，有 'brute'、'kd_tree' 和 'ball_tree'。
    leaf_size 是 kd_tree 或 ball_tree 生成的树的树叶（树叶就是二叉树中没有分枝的节点）的大小。
    metric 和 p，是我们在 kNN 入门文章中介绍过的距离函数的选项，如果 metric ='minkowski' 并且 p=p 的话，计算两点之间的距离就是
    d((x1,…,xn),(y1,…,yn))=(∑i=1n|xi−yi|p)1/p
    d((x1,…,xn),(y1,…,yn))=(∑i=1n|xi−yi|p)1/p
    一般来讲，默认的 metric='minkowski'（默认）和 p=2（默认）就可以满足大部分需求。其他的 metric 选项可见说明文档。
    n_jobs 是并行计算的线程数量，默认是 1，输入 -1 则设为 CPU 的内核数。

    """
    knn.fit(fv, cla) #导入数据进行训练
    test = [test]  # 转为二维数组符合新写法
    prediction = knn.predict(test)
    return test, prediction

def indexTwoKNN():
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

    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
                                         algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None,
                                         n_jobs=1)  # 取得knn分类器
    knn.fit(fv, cla)  # 导入数据进行训练
    test = [test]  # 转为二维数组符合新写法
    prediction = knn.predict(test)
    return test, prediction

#指标组三
def indexThreeKNN():
    x = 10 ** 7
    #表查询语句
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv = []  #特征
    for res in result1:
        a = []
        a.append(float(list(res)[3]))
        a.append(float(list(res)[5])/x)  #对数据进行归一化
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
        test.append(float(list(res)[5])/x)

    fv = np.array(fv)
    cla = np.array(cla)
    test = np.array(test)

    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
                                         algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None,
                                         n_jobs=1)  # 取得knn分类器
    knn.fit(fv, cla)  # 导入数据进行训练
    test = [test]  # 转为二维数组符合新写法
    prediction = knn.predict(test)
    return test, prediction


def main():
    time1 = time.time()

    test1, pre1 = indexOneKNN()
    print (u"kNN邻近指标组一：", test1, pre1)
    test2, pre2 = indexTwoKNN()
    print (u"kNN邻近指标组二：", test2, pre2)
    test3, pre3 = indexThreeKNN()
    print (u"kNN邻近指标组三：", test3, pre3)

    time2 = time.time()
    print ((time2 - time1)/60, u"分钟")



if __name__ == '__main__':
    main()