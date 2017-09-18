# -*- coding=utf-8 -*-
# Tensorboard

"""
运行该段代码后，若要在win下查看则需首先在cmd中切换到logs目录下，之后运行
tensorboard --logdir=E:\logs
然后cmd窗口会出现“卡死”的状态，不必惊慌，只需打开Chrome浏览器
访问http://localhost:6006即可查看
"""

import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

# relu sigmoid 常用的激励函数
def add_layer(inputs, in_size, out_size, activation_function=None):
    with tf.name_scope('layer'):
        with tf.name_scope('weights'):
            Weights = tf.Variable(tf.random_normal([in_size, out_size]), name='W')
        with tf.name_scope('biases'):
            biases = tf.Variable(tf.zeros([1, out_size]) + 0.1, name='b')
        with tf.name_scope('Wx_plus_b'):
            Wx_plus_b = tf.matmul(inputs, Weights) + biases
        if activation_function is None:
            outputs = Wx_plus_b
        else:
            outputs = activation_function(Wx_plus_b,)
        return outputs

# 生成数据
x_data = np.linspace(-1, 1, 300)[:, np.newaxis]
noise = np.random.normal(0, 0.05, x_data.shape)
y_data = np.square(x_data) - 0.5 + noise

with tf.name_scope('inputs'):
    xs = tf.placeholder(tf.float32, [None, 1], name='x_input')
    ys = tf.placeholder(tf.float32, [None, 1], name='y_input')

# 隐藏层
l1 = add_layer(xs, 1, 10, activation_function=tf.nn.relu)
# 输出层
prediction = add_layer(l1, 10, 1, activation_function=None)

# loss是prediction与y_data的差别
with tf.name_scope('loss'):
    loss = tf.reduce_mean(tf.reduce_sum(tf.square(ys - prediction), reduction_indices=[1]), name='loss')

# 训练
with tf.name_scope('train'):
    train_step = tf.train.GradientDescentOptimizer(0.2).minimize(loss)


sess = tf.Session()
writer = tf.summary.FileWriter("logs/", sess.graph)
init = tf.global_variables_initializer()
sess.run(init)










