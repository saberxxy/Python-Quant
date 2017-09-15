# -*- coding=utf-8 -*-
# TensorFlow的变量

import tensorflow as tf
import numpy as np

# 定义变量
state = tf.Variable(0, name='counter')
one = tf.constant(1)
new_value = tf.add(state, one)
update = tf.assign(state, new_value)

# 激活变量
init = tf.initialize_all_variables()

# run
with tf.Session() as sess:
    sess.run(init)
    for _ in range(3):
        sess.run(update)
        print(sess.run(state))