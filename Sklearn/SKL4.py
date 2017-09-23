#-*- coding=utf-8 -*-
# scikit-learn overfitting2 SVC参数调优

from sklearn.model_selection import validation_curve
from sklearn.datasets import load_digits
from sklearn.svm import SVC
import matplotlib.pyplot as plt
import numpy as np

digits = load_digits()
X = digits.data
y = digits.target
param_range = np.logspace(-6, -2.3, 5)
train_loss, test_loss = validation_curve(
        SVC(), X, y, param_name='gamma', param_range=param_range, scoring='neg_mean_squared_error')
train_loss_mean = -np.mean(train_loss, axis=1)
test_loss_mean = -np.mean(test_loss, axis=1)

plt.plot(param_range, train_loss_mean, 'o-', color="pink", label="Training")
plt.plot(param_range, test_loss_mean, 'o-', color="blue", label="Cross-validation")

plt.xlabel("gamma's Value")
plt.ylabel("Loss")
plt.legend(loc="best")
plt.show()


