#-*- coding=utf-8 -*-
# scikit-learn 保存model

from sklearn import svm
from sklearn import datasets
import pickle
from sklearn.externals import joblib

clf = svm.SVC()
iris = datasets.load_iris()
X, y = iris.data, iris.target
clf.fit(X, y)

# 保存1
with open('save/cil.pikle', 'wb') as f:
    pickle.dump(clf, f)

# 读取1
with open('save/cil.pikle', 'rb') as f:
    clf2 = pickle.load(f)
    print(clf2.predict(X[0:1]))

# 保存2
joblib.dump(clf, 'save/clf.pkl')

# 读取2
clf3 = joblib.load('save/clf.pkl')
print(clf3.predict(X[0:1]))
