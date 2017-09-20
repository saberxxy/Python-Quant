# -*- coding=utf-8 -*-
# 保存和读取训练好的神经网络

import tensorflow as tf
import numpy as np

""""
注：目前的TensorFlow只能保存训练好的神经网络的变量，不能保存整个的网络
"""

# 将训练好的神经网络保存为文件
W = tf.Variable([[1,2,3],[3,4,5]], dtype=tf.float32, name='weights')
b = tf.Variable([[1,2,3]], dtype=tf.float32, name='biases')

init = tf.global_variables_initializer()

saver = tf.train.Saver()

with tf.Session() as sess:
    sess.run(init)
    save_path = saver.save(sess, 'my_ann/save_net.ckpt')
    print(save_path)

#------------------------------------------------------------------------------

# 读取保存的神经网络
W = tf.Variable(np.arange(6).reshape(2,3), dtype=tf.float32, name='weights')
b = tf.Variable(np.arange(3).reshape(1,3), dtype=tf.float32, name='biases')

saver = tf.train.Saver()

with tf.Session() as sess:
    saver.restore(sess, 'my_ann/save_net.ckpt')
    print(sess.run(W))
    print(sess.run(b))