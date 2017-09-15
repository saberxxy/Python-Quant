# -*- coding=utf-8 -*-
# TensorFlow测试

import tensorflow as tf
import numpy as np

# 造随机数，并定制为float32，因为在TensorFlow中数据结构的类型大都为次
x_data = np.random.rand(100).astype(np.float32)
y_data = x_data*0.1 + 0.3

# 创建计算图谱
Weights = tf.Variable(tf.random_uniform([1], -1.0, 1.0))  # 权重
biases = tf.Variable(tf.zeros([1]))  # 偏置

y = Weights*x_data + biases

loss = tf.reduce_mean(tf.square(y-y_data))  # 误差
# 优化器，减少误差，此处选择的优化器为梯度下降优化器，参数为学习效率，一般是小于1的数
optimizer = tf.train.GradientDescentOptimizer(0.5)
train = optimizer.minimize(loss)

init = tf.initialize_all_variables()

# 激活神经网络
sess = tf.Session()
sess.run(init)

# 训练神经网络
for step in range(0, 201):
    sess.run(train)
    if step % 20 == 0:
        print(step, sess.run(Weights), sess.run(biases))