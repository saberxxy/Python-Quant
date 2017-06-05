#-*- coding=utf-8 -*-
#前馈神经网络拟合

import sys
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.datasets import SupervisedDataSet
from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer
from pybrain.structure import FullConnection
import pymysql
import time

mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = 'root'
mysqlDatabaseName = 'test'
mysqlPort = 3306
connStr = 'mysql://root:root@127.0.0.1:3306/test?charset=utf8'
conn = pymysql.connect(host=mysqlHost, user=mysqlUser, passwd=mysqlPassword, db=mysqlDatabaseName, port=mysqlPort, charset="utf8")

def ffn(nodesNum, trainingTime):
    """构建神经网络"""
    n = FeedForwardNetwork()

    inLayer = LinearLayer(6)  #构建神经网络的三层
    hiddenLayer1 = SigmoidLayer(nodesNum)
    hiddenLayer2 = SigmoidLayer(nodesNum)
    hiddenLayer3 = SigmoidLayer(nodesNum)
    hiddenLayer4 = SigmoidLayer(nodesNum)
    hiddenLayer5 = SigmoidLayer(nodesNum)
    hiddenLayer6 = SigmoidLayer(nodesNum)
    hiddenLayer7 = SigmoidLayer(nodesNum)
    hiddenLayer8 = SigmoidLayer(nodesNum)
    hiddenLayer9 = SigmoidLayer(nodesNum)
    hiddenLayer10 = SigmoidLayer(nodesNum)
    outLayer = LinearLayer(1)

    n.addInputModule(inLayer)  #将三层加入网络中
    n.addModule(hiddenLayer1)
    n.addModule(hiddenLayer2)
    n.addModule(hiddenLayer3)
    n.addModule(hiddenLayer4)
    n.addModule(hiddenLayer5)
    n.addModule(hiddenLayer6)
    n.addModule(hiddenLayer7)
    n.addModule(hiddenLayer8)
    n.addModule(hiddenLayer9)
    n.addModule(hiddenLayer10)
    n.addOutputModule(outLayer)

    in_to_hidden = FullConnection(inLayer, hiddenLayer1)  #设置连接模式
    hidden_to_hidden1 = FullConnection(hiddenLayer1, hiddenLayer2)
    hidden_to_hidden2 = FullConnection(hiddenLayer2, hiddenLayer3)
    hidden_to_hidden3 = FullConnection(hiddenLayer3, hiddenLayer4)
    hidden_to_hidden4 = FullConnection(hiddenLayer4, hiddenLayer5)
    hidden_to_hidden5 = FullConnection(hiddenLayer5, hiddenLayer6)
    hidden_to_hidden6 = FullConnection(hiddenLayer6, hiddenLayer7)
    hidden_to_hidden7 = FullConnection(hiddenLayer7, hiddenLayer8)
    hidden_to_hidden8 = FullConnection(hiddenLayer8, hiddenLayer9)
    hidden_to_hidden9 = FullConnection(hiddenLayer9, hiddenLayer10)
    hidden_to_out = FullConnection(hiddenLayer10, outLayer)

    n.addConnection(in_to_hidden)  #将连接加入网络
    n.addConnection(hidden_to_hidden1)
    n.addConnection(hidden_to_hidden2)
    n.addConnection(hidden_to_hidden3)
    n.addConnection(hidden_to_hidden4)
    n.addConnection(hidden_to_hidden5)
    n.addConnection(hidden_to_hidden6)
    n.addConnection(hidden_to_hidden7)
    n.addConnection(hidden_to_hidden8)
    n.addConnection(hidden_to_hidden9)
    n.addConnection(hidden_to_out)

    n.sortModules()  #使网络可用
    print (n)

    """建立数据集"""
    ds = SupervisedDataSet(6, 1)  #六个输入，一个输出

    #表查询语句
    cur1 = conn.cursor()
    cur1.execute('select * from szzs_rise_and_fall_rate limit 2,9999999999999999;')
    result1 = cur1.fetchall()
    fv = []  #特征
    for res in result1:
        a = []
        a.append(float(list(res)[1]))
        a.append(float(list(res)[2]))
        a.append(float(list(res)[3]))
        a.append(float(list(res)[4]))
        a.append(float(list(res)[5]))
        a.append(float(list(res)[6]))
        fv.append(a)

    cur2 = conn.cursor()
    cur2.execute('select rise_fall_rate_next from szzs_rise_and_fall_rate limit 2,9999999999999999;')
    result2 = cur2.fetchall()
    cla = []  #分类
    for res in result2:
        cla.append(float(list(res)[0]))

    cur3 = conn.cursor()
    cur3.execute('select * from szzs_rise_and_fall_rate order by date desc;')
    result3 = cur3.fetchmany(1)
    test = []  #测试数据
    for res in result3:
        test.append(float(list(res)[1]))
        test.append(float(list(res)[2]))
        test.append(float(list(res)[3]))
        test.append(float(list(res)[4]))
        test.append(float(list(res)[5]))
        test.append(float(list(res)[6]))

    for i in range(0, len(fv)):
        ds.addSample(fv[i], cla[i])

    dataTrain, dataTest = ds.splitWithProportion(0.8)  #百分之八十的数据用于训练，百分之二十的数据用于测试

    """训练神经网络"""
    trainer = BackpropTrainer(n, dataset=dataTrain)  #神经网络和数据集
    trainer.trainEpochs(trainingTime)  #训练次数
    return n.activate(test)


def main():
    time1 = time.time()
    nodesNum = 10
    trainingTime = 1000
    pre = ffn(nodesNum, trainingTime)
    print u'预测值：', pre

    time2 = time.time()
    print (time2 - time1)/60, u"分钟"



if __name__ == '__main__':
    main()


