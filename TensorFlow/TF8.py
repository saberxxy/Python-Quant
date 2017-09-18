# -*- coding=utf-8 -*-
# Tensorflow分类算法

# 分类器，回归器

"""
运行该段代码后，若要在win下查看则需首先在cmd中切换到logs目录下，之后运行
tensorboard --logdir=E:\Program\Python\Quant\TensorFlow\logs
然后cmd窗口会出现“卡死”的状态，不必惊慌，只需打开Chrome浏览器
访问http://localhost:6006即可查看
"""

import tensorflow as tf
import numpy as np
from tensorflow.examples.tutorials.mnist import input_data


# 构造一层神经网络
def add_layer(inputs, in_size, out_size, n_layer, activation_function=None):
    layer_name = 'layer%s' % n_layer
    with tf.name_scope(layer_name):
        with tf.name_scope('weights'):
            Weights = tf.Variable(tf.random_normal([in_size, out_size]), name='W')
            # 在histograms中显示的用tf.summary.histogram()函数
            tf.summary.histogram(layer_name + '/weights', Weights)
        with tf.name_scope('biases'):
            biases = tf.Variable(tf.zeros([1, out_size]) + 0.1, name='b')
            tf.summary.histogram(layer_name + '/biases', biases)
        with tf.name_scope('Wx_plus_b'):
            Wx_plus_b = tf.matmul(inputs, Weights) + biases
        if activation_function is None:
            outputs = Wx_plus_b
        else:
            outputs = activation_function(Wx_plus_b,)
            tf.summary.histogram(layer_name + '/outputs', outputs)
        return outputs

# 计算准确度
def compute_accuracy(v_xs, v_ys):
    global prediction
    y_pre = sess.run(prediction, feed_dict={xs:v_xs})
    correct_predicition = tf.equal(tf.argmax(y_pre, 1), tf.argmax(v_ys, 1))
    accuracy = tf.reduce_mean(tf.cast(correct_predicition, tf.float32))
    result = sess.run(accuracy, feed_dict={xs:v_xs, ys:v_ys})
    return result


mnist = input_data.read_data_sets('MNIST_data', one_hot=True)

xs = tf.placeholder(tf.float32, [None, 784])
ys = tf.placeholder(tf.float32, [None, 10])

sess = tf.Session()

# 输出层
prediction = add_layer(xs, 784, 10, n_layer='outputLayer', activation_function=tf.nn.softmax)

with tf.name_scope('cross_entropy'):
    cross_entropy = tf.reduce_mean(-tf.reduce_sum(ys*tf.log(prediction),
                                        reduction_indices=[1]), name='cross_entropy')
with tf.name_scope('train'):
    train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

merged = tf.summary.merge_all()
writer = tf.summary.FileWriter("logs/", sess.graph)
sess.run(tf.global_variables_initializer())

for i in range(1000):
    batch_xs, batch_ys = mnist.train.next_batch(100)  # 每次抽取100个进行学习
    sess.run(train_step, feed_dict={xs:batch_xs, ys:batch_ys})
    result = sess.run(merged, feed_dict={xs:batch_xs, ys:batch_ys})
    if i%50 == 0:
        print(compute_accuracy(mnist.test.images, mnist.test.labels))
    writer.add_summary(result, i)




