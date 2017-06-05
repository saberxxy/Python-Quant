#-*- coding=utf-8 -*-
#获取数据、清洗数据、涨跌分类分析

import pymysql
import datetime
import configparser
import sys
import threading
import queue
from sklearn import svm
from sklearn import neighbors
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.neural_network import MLPClassifier

#读取配置文件
cf = configparser.ConfigParser()
cf.read("config.conf")
mysqlHost = str(cf.get("mysql", "ip"))
mysqlPort = int(cf.get("mysql", "port"))
mysqlUser = str(cf.get("mysql", "username"))
mysqlPassword = str(cf.get("mysql", "password"))
mysqlDatabaseName = str(cf.get("mysql", "databasename"))
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'

#建立数据库连接
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                        db=mysqlDatabaseName, port=mysqlPort, charset="utf8")
print (conn)
cur1 = conn.cursor()

# 建立队列
q = queue.Queue()

# 获取训练数据和测试数据
def getTraningAndTestData():
    # 指标组一的数据
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv1 = []  # 特征，训练数据
    for res in result1:
        a = []
        a.append(float(list(res)[1]))  # 开盘价
        a.append(float(list(res)[2]))  # 最高价
        a.append(float(list(res)[3]))  # 收盘价
        a.append(float(list(res)[4]))  # 最低价
        a.append(int(list(res)[5]))  # 成交量
        a.append(int(list(res)[6]))  # 成交金额
        a.append(float(list(res)[7]))  # 前收盘
        fv1.append(a)
    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla1 = []  # 分类数据，0为跌，1为涨
    for res in result2:
        cla1.append(int(list(res)[0]))
    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test1 = []  # 测试数据，对应上述训练数据的结构
    for res in result3:
        test1.append(float(list(res)[1]))
        test1.append(float(list(res)[2]))
        test1.append(float(list(res)[3]))
        test1.append(float(list(res)[4]))
        test1.append(int(list(res)[5]))
        test1.append(int(list(res)[6]))
        test1.append(float(list(res)[7]))

    # 指标组二的数据
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv2 = []  # 特征
    for res in result1:
        a = []
        a.append(float(list(res)[1]))
        a.append(float(list(res)[2]))
        a.append(float(list(res)[3]))
        a.append(float(list(res)[4]))
        fv2.append(a)
    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla2 = []  # 分类
    for res in result2:
        cla2.append(int(list(res)[0]))
    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test2 = []  # 测试数据
    for res in result3:
        test2.append(float(list(res)[1]))
        test2.append(float(list(res)[2]))
        test2.append(float(list(res)[3]))
        test2.append(float(list(res)[4]))

    # 指标组三的数据
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_black_swan limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv3 = []  # 特征
    for res in result1:
        a = []
        a.append(float(list(res)[3]))
        a.append(float(list(res)[5]))
        fv3.append(a)
    cur2 = conn.cursor()
    cur2.execute('select rise_fall_next from szzs_black_swan limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla3 = []  # 分类
    for res in result2:
        cla3.append(int(list(res)[0]))
    cur3 = conn.cursor()
    cur3.execute('select * from szzs_black_swan order by date desc;')
    result3 = cur3.fetchmany(1)
    test3 = []  # 测试数据
    for res in result3:
        test3.append(float(list(res)[3]))
        test3.append(float(list(res)[5]))

    return fv1, cla1, test1, fv2, cla2, test2, fv3, cla3, test3

# 线程一
def worker1(fv1, cla1, test1):
    func_name = sys._getframe().f_code.co_name
    clf = svm.SVC()  # 支持向量机
    clf.fit(fv1, cla1)
    prediction = clf.predict(test1)
    q.put((prediction, func_name))

# 线程二
def worker2(fv2, cla2, test2):
    func_name = sys._getframe().f_code.co_name
    clf = svm.SVC()  # 支持向量机
    clf.fit(fv2, cla2)
    prediction = clf.predict(test2)
    q.put((prediction, func_name))

# 线程三
def worker3(fv3, cla3, test3):
    func_name = sys._getframe().f_code.co_name
    clf = svm.SVC()  # 支持向量机
    clf.fit(fv3, cla3)
    prediction = clf.predict(test3)
    q.put((prediction, func_name))

# 线程四
def worker4(fv1, cla1, test1):
    func_name = sys._getframe().f_code.co_name
    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
                                         algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None,
                                         n_jobs=1)  # 取得knn分类器
    knn.fit(fv1, cla1)  # 导入数据进行训练
    prediction = knn.predict(test1)
    q.put((prediction, func_name))

# 线程五
def worker5(fv2, cla2, test2):
    func_name = sys._getframe().f_code.co_name
    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
                                         algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None,
                                         n_jobs=1)  # 取得knn分类器
    knn.fit(fv2, cla2)  # 导入数据进行训练
    prediction = knn.predict(test2)
    q.put((prediction, func_name))

# 线程六
def worker6(fv3, cla3, test3):
    func_name = sys._getframe().f_code.co_name
    knn = neighbors.KNeighborsClassifier(n_neighbors=30, weights='uniform',
                                         algorithm='auto', leaf_size=30, p=2, metric='minkowski', metric_params=None,
                                         n_jobs=1)  # 取得knn分类器
    knn.fit(fv3, cla3)  # 导入数据进行训练
    prediction = knn.predict(test3)
    q.put((prediction, func_name))

# 线程七
def worker7(fv1, cla1, test1):
    func_name = sys._getframe().f_code.co_name
    adaBoost = AdaBoostClassifier(base_estimator=None, n_estimators=50,
                                  learning_rate=1.0, algorithm='SAMME.R', random_state=None)
    adaBoost.fit(fv1, cla1)  # 导入数据进行训练
    prediction = adaBoost.predict(test1)
    q.put((prediction, func_name))

# 线程八
def worker8(fv2, cla2, test2):
    func_name = sys._getframe().f_code.co_name
    adaBoost = AdaBoostClassifier(base_estimator=None, n_estimators=50,
                                  learning_rate=1.0, algorithm='SAMME.R', random_state=None)
    adaBoost.fit(fv2, cla2)  # 导入数据进行训练
    prediction = adaBoost.predict(test2)
    q.put((prediction, func_name))

# 线程九
def worker9(fv3, cla3, test3):
    func_name = sys._getframe().f_code.co_name
    adaBoost = AdaBoostClassifier(base_estimator=None, n_estimators=50,
                                  learning_rate=1.0, algorithm='SAMME.R', random_state=None)
    adaBoost.fit(fv3, cla3)  # 导入数据进行训练
    prediction = adaBoost.predict(test3)
    q.put((prediction, func_name))

# 线程十
def worker10(fv1, cla1, test1):
    func_name = sys._getframe().f_code.co_name
    mlp = MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(1000, 200), random_state=1, batch_size='auto')
    mlp.fit(fv1, cla1)  # 导入数据进行训练
    prediction = mlp.predict(test1)
    q.put((prediction, func_name))

# 线程十一
def worker11(fv2, cla2, test2):
    func_name = sys._getframe().f_code.co_name
    mlp = MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(1000, 200), random_state=1, batch_size='auto')
    mlp.fit(fv2, cla2)  # 导入数据进行训练
    prediction = mlp.predict(test2)
    q.put((prediction, func_name))

# 线程十二
def worker12(fv3, cla3, test3):
    func_name = sys._getframe().f_code.co_name
    mlp = MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(1000, 200), random_state=1, batch_size='auto')
    mlp.fit(fv3, cla3)  # 导入数据进行训练
    prediction = mlp.predict(test3)
    q.put((prediction, func_name))

# 主函数
def main():
    # gd.getData()
    # gbsd.blackSwanCln()
    #
    # tableName1 = 'szzs_black_swan'
    # tableName2 = 'predict_rise_and_fall'
    #
    # cur1.execute('select date, rise_fall from %s order by date desc' % (tableName1))
    # content1 = cur1.fetchone()
    #
    # cur1.execute(
    #     'update predict_rise_and_fall set rise_and_fall = "%d" where date="%s" ' % (int(content1[1]), content1[0]))
    # cur1.execute('commit')
    # print('更新前一交易日涨跌情况完毕')

    # 获取全部数据
    fv1, cla1, test1, fv2, cla2, test2, fv3, cla3, test3 = getTraningAndTestData()
    # worker8(fv1, cla1, test1)

    result = list()
    t1 = threading.Thread(target=worker1, name='thread1', args=(fv1, cla1, test1,))
    t2 = threading.Thread(target=worker2, name='thread2', args=(fv2, cla2, test2,))
    t3 = threading.Thread(target=worker3, name='thread3', args=(fv3, cla3, test3,))
    t4 = threading.Thread(target=worker4, name='thread4', args=(fv1, cla1, test1,))
    t5 = threading.Thread(target=worker5, name='thread5', args=(fv2, cla2, test2,))
    t6 = threading.Thread(target=worker6, name='thread6', args=(fv3, cla3, test3,))
    t7 = threading.Thread(target=worker7, name='thread7', args=(fv1, cla1, test1,))
    t8 = threading.Thread(target=worker8, name='thread8', args=(fv2, cla2, test2,))
    t9 = threading.Thread(target=worker9, name='thread9', args=(fv3, cla3, test3,))
    t10 = threading.Thread(target=worker10, name='thread10', args=(fv1, cla1, test1,))
    t11 = threading.Thread(target=worker11, name='thread11', args=(fv2, cla2, test2,))
    t12 = threading.Thread(target=worker12, name='thread12', args=(fv3, cla3, test3,))
    print('-' * 50)

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()
    t11.start()
    t12.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    t9.join()
    t10.join()
    t11.join()
    t12.join()

    while not q.empty():
        result.append(q.get())
    for item in result:
        print(item)
        if item[1] == worker1.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker2.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker3.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker4.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker5.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker6.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker7.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker8.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker9.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker10.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker11.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))
        elif item[1] == worker12.__name__:
            print ("%s 's return value is : %s" % (item[1], item[0]))



if __name__ == '__main__':
    main()
