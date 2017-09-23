#-*- coding=utf-8 -*-
# scikit-learn 交叉验证

import numpy as np
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.neighbors import KNeighborsClassifier
import matplotlib.pyplot as plt

iris = datasets.load_iris()
iris_X = iris.data
iris_y = iris.target
X_train, X_test, y_train, y_test = train_test_split(iris_X, iris_y, test_size=0.2)
X = iris.data
y = iris.target

k_range = range(1, 31)
k_scores = []

for k in k_range:
    knn = KNeighborsClassifier(n_neighbors=k)
    scores = cross_val_score(knn, X, y, cv=10, scoring='accuracy')  # cv代表将数据集分为几个set
    loss = -cross_val_score(knn, X, y, cv=10, scoring='neg_mean_squared_error')
    k_scores.append(scores.mean())

plt.plot(k_range, k_scores)
plt.xlabel('K Value')
plt.ylabel('Accuracy')
plt.show()

# 通过实验发现，K值的选择一般12——18个为宜，不必过多


# print(X_train)
# knn.fit(X_train, y_train)
# print(scores.mean())
