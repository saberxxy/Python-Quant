#-*- coding=utf-8 -*-
#获取数据、清洗数据、涨跌分类分析

import pymysql
import datetime
import configparser
import time

from Quant.Sklearn import GetData as gd
from Quant.Sklearn import GetBlackSwanData as gbsd
from Quant.Sklearn import RandomTreeRiseAndFall as rtraf
from Quant.Sklearn import AdaBoost as ab
from Quant.Sklearn import SVMRiseAndFall as svmraf
from Quant.Sklearn import KnnRiseAndFall as knnraf
from Quant.Sklearn import MLP as mlp
from Quant.Sklearn import LogisticRegression as lg
from Quant.Sklearn import StochasticGradientDescent as sgd

#读取配置文件
cf = configparser.ConfigParser()
cf.read(u'E:/Program/Python/Quant/Sklearn/config.conf')
mysqlHost = str(cf.get("mysql", "ip"))
mysqlPort = int(cf.get("mysql", "port"))
mysqlUser = str(cf.get("mysql", "username"))
mysqlPassword = str(cf.get("mysql", "password"))
mysqlDatabaseName = str(cf.get("mysql", "databasename"))

#建立数据库连接
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword,
                        db=mysqlDatabaseName, port=mysqlPort, charset="utf8")
print (conn)
cur1 = conn.cursor()

def main():
    time1 = time.time()

    gd.getData()
    gbsd.blackSwanCln()

    tableName1 = 'szzs_black_swan'
    tableName2 = 'predict_rise_and_fall'

    cur1.execute('select date, rise_fall from %s order by date desc' % (tableName1))
    content1 = cur1.fetchone()
    # print (content1[0], int(content1[1])) #时间和涨跌情况

    cur1.execute('update predict_rise_and_fall set rise_and_fall = "%d" where date="%s" ' % (int(content1[1]), content1[0]))
    cur1.execute('commit')
    print ('更新前一交易日涨跌情况完毕')

    # test4, pre4 = nb.indexOneNB()
    # print('朴素贝叶斯，指标组一', test4, pre4)
    # test5, pre5 = nb.indexTwoNB()
    # print('朴素贝叶斯，指标组二', test5, pre5)
    # test6, pre6 = nb.indexThreeNB()
    # print('朴素贝叶斯，指标组三', test6, pre6)

    test7, pre7 = svmraf.indexOneSVM()
    print('支持向量机，指标组一', test7, pre7)
    test8, pre8 = svmraf.indexTwoSVM()
    print('支持向量机，指标组二', test8, pre8)
    test9, pre9 = svmraf.indexThreeSVM()
    print('支持向量机，指标组三', test9, pre9)

    test10, pre10 = knnraf.indexOneKNN()
    print('kNN，指标组一', test10, pre10)
    test11, pre11 = knnraf.indexTwoKNN()
    print('kNN，指标组二', test11, pre11)
    test12, pre12 = knnraf.indexThreeKNN()
    print('kNN，指标组三', test12, pre12)

    treeNumber = 10000
    test1, pre1 = rtraf.indexOneRT(treeNumber)
    print('随机森林，指标组一', test1, pre1)
    test2, pre2 = rtraf.indexTwoRT(treeNumber)
    print('随机森林，指标组二', test2, pre2)
    test3, pre3 = rtraf.indexThreeRT(treeNumber)
    print('随机森林，指标组三', test3, pre3)

    test13, pre13 = ab.indexOneAdaBoost()
    print('AdaBoost，指标组一', test13, pre13)
    test14, pre14 = ab.indexTwoAdaBoost()
    print('AdaBoost，指标组二', test14, pre14)
    test15, pre15 = ab.indexThreeAdaBoost()
    print('AdaBoost，指标组三', test15, pre15)

    test16, pre16 = mlp.indexOneMLP()
    print('MLP，指标组一', test16, pre16)
    test17, pre17 = mlp.indexTwoMLP()
    print('MLP，指标组二', test17, pre17)
    test18, pre18 = mlp.indexThreeMLP()
    print('MLP，指标组三', test18, pre18)

    test19, pre19 = lg.indexOneLR()
    print('Logistic回归，指标组一', test19, pre19)
    test20, pre20 = lg.indexTwoLR()
    print('Logistic回归，指标组二', test20, pre20)
    test21, pre21 = lg.indexThreeLR()
    print('Logistic回归，指标组三', test21, pre21)

    test22, pre22 = sgd.indexOneSGD()
    print('随机梯度下降，指标组一', test22, pre22)
    test23, pre23 = sgd.indexTwoSGD()
    print('随机梯度下降，指标组二', test23, pre23)
    test24, pre24 = sgd.indexThreeSGD()
    print('随机梯度下降，指标组三', test24, pre24)

    print ('计算完毕')

    cur1.execute('select date, rise_fall from %s order by date desc' % (tableName1))
    content1 = cur1.fetchone()

    cur1.execute("insert into %s(date, rf_1, rf_2, rf_3, svm_1, svm_2, svm_3, knn_1, knn_2, knn_3, "
                 "ada_1, ada_2, ada_3, mlp_1, mlp_2, mlp_3, lg_1, lg_2, lg_3, sgd_1, sgd_2, sgd_3)"
                 "values('%s', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', "
                 " '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d')"
                 % (tableName2, str(content1[0]+datetime.timedelta(days=1)), pre1[0], pre2[0], pre3[0], pre7[0], pre8[0],
                    pre9[0], pre10[0], pre11[0], pre12[0], pre13[0], pre14[0], pre15[0], pre16[0], pre17[0], pre18[0],
                    pre19[0], pre20[0], pre21[0], pre22[0], pre23[0], pre24[0]))
    cur1.execute('commit')
    print (u'结果插入完毕')

    time2 = time.time()
    print((time2 - time1) / 60, u"分钟")

if __name__ == '__main__':
    main()


