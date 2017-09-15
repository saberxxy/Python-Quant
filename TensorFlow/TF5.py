# -*- coding=utf-8 -*-
# TensorFlow神经网络结果的可视化

import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

# relu sigmoid 常用的激励函数
def add_layer(inputs, in_size, out_size, activation_function=None):
    Weights = tf.Variable(tf.random_normal([in_size, out_size]))
    biases = tf.Variable(tf.zeros([1, out_size]) + 0.1)
    Wx_plus_b = tf.matmul(inputs, Weights) + biases
    if activation_function is None:
        outputs = Wx_plus_b
    else:
        outputs = activation_function(Wx_plus_b)
    return outputs

# 生成数据
x_data = np.linspace(-1, 1, 300)[:, np.newaxis]
noise = np.random.normal(0, 0.05, x_data.shape)
y_data = np.square(x_data) - 0.5 + noise

xs = tf.placeholder(tf.float32, [None, 1])
ys = tf.placeholder(tf.float32, [None, 1])

# 隐藏层
l1 = add_layer(xs, 1, 10, activation_function=tf.nn.relu)
# 输出层
prediction = add_layer(l1, 10, 1, activation_function=None)

# loss是prediction与y_data的差别
loss = tf.reduce_mean(tf.reduce_sum(tf.square(ys - prediction), reduction_indices=[1]))

# 训练
train_step = tf.train.GradientDescentOptimizer(0.2).minimize(loss)

init = tf.global_variables_initializer()
sess = tf.Session()
sess.run(init)

# 结果可视化
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.scatter(x_data, y_data)
plt.ion()
plt.show()

for i in range(100):
    sess.run(train_step, feed_dict={xs:x_data, ys:y_data})
    # 打印每一次预测的误差
    # print(sess.run(loss, feed_dict={xs:x_data, ys:y_data}))
    try:
        ax.lines.remove(lines[0])
    except Exception:
        pass
    prediction_value = sess.run(prediction, feed_dict={xs:x_data})
    lines = ax.plot(x_data, prediction_value, 'red', lw=5)
    plt.pause(0.1)





