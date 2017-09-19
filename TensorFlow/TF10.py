# -*- coding=utf-8 -*-
# 卷积神经网络

import tensorflow as tf
import numpy as np
from tensorflow.examples.tutorials.mnist import input_data

# 计算误差
def compute_accuracy(v_xs, v_ys):
    global prediction
    y_pre = sess.run(prediction, feed_dict={xs:v_xs, keep_prob:1})
    correct_predicition = tf.equal(tf.argmax(y_pre, 1), tf.argmax(v_ys, 1))
    accuracy = tf.reduce_mean(tf.cast(correct_predicition, tf.float32))
    result = sess.run(accuracy, feed_dict={xs:v_xs, ys:v_ys, keep_prob:1})
    return result

# 权重
def weight_variable(shape):
    inital = tf.truncated_normal(shape, stddev=0.1)
    return tf.Variable(inital)

# 偏置
def bias_variable(shape):
    inital = tf.constant(0.1, shape=shape)
    return tf.Variable(inital)

# 卷积层
def conv2d(x, W):
    # strides[1, x_movement, y_movement, 1]
    # 第一个和最后一个要为1
    return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')

# 池化
def max_pool_2x2(x):
    return tf.nn.max_pool(x, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME')

# 神经网络的输入参数
xs = tf.placeholder(tf.float32, [None, 784])/255  # 28*28
ys = tf.placeholder(tf.float32, [None, 10])
keep_prob = tf.placeholder(tf.float32)
x_image = tf.reshape(xs, [-1, 28, 28, 1])
print('输入参数构建完毕')

# 卷积层1
W_conv1 = weight_variable([5, 5, 1, 32])  # patch为5*5，输入是1，输出是32
b_conv1 = bias_variable([32])
h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)  # 使用relu函数进行非线性化处理
h_pool1 = max_pool_2x2(h_conv1)  # 池化
print('卷积层1构建完毕')

# 卷积层2
W_conv2 = weight_variable([5, 5, 32, 64])
b_conv2 = bias_variable([64])
h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)  # 使用relu函数进行非线性化处理
h_pool2 = max_pool_2x2(h_conv2)  # 池化
print('卷积层2构建完毕')

# 全连接层1
W_fc1 = weight_variable([7*7*64, 1024])
b_fc1 = bias_variable([1024])
h_pool2_flat = tf.reshape(h_pool2, [-1, 7*7*64])  # 将三维的形状变为扁平
h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)
h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)
print('全连接层1构建完毕')

# 全连接层2
W_fc2 = weight_variable([1024, 10])
b_fc2 = bias_variable([10])
prediction = tf.nn.softmax(tf.matmul(h_fc1_drop, W_fc2) + b_fc2)
print('全连接层2构建完毕')

mnist = input_data.read_data_sets('MNIST_data', one_hot=True)
print('数据读取完毕')

cross_entropy = tf.reduce_mean(-tf.reduce_sum(ys*tf.log(prediction), reduction_indices=[1]))
train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)

sess = tf.Session()
sess.run(tf.global_variables_initializer())
print('神经网络启动完毕')

for i in range(1000):
    batch_xs, batch_ys = mnist.train.next_batch(100)  # 每次抽取100个进行学习
    sess.run(train_step, feed_dict={xs: batch_xs, ys: batch_ys, keep_prob: 0.5})
    if i % 50 == 0:
        print(compute_accuracy(mnist.test.images[:1000], mnist.test.labels[:1000]))