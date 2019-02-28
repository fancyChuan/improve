# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2018/1/30 17:58
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
from __future__ import print_function
from sklearn import datasets, cross_validation, naive_bayes
import numpy as np
import matplotlib.pyplot as plt

digits = datasets.load_digits()

def show_digits():
    plt.figure()
    plt.imshow(digits.images[0], cmap=plt.cm.gray_r, interpolation='nearest') # todo: 没搞懂什么用法
    plt.show()

def load_data():
    return cross_validation.train_test_split(digits.data, digits.target, test_size=0.25, random_state=0)


X_train, X_test, y_train, y_test = load_data()

def test_GaussianNB():
    # X_train, X_test, y_train, y_test = load_data()
    cls = naive_bayes.GaussianNB()
    cls.fit(X_train, y_train)

    print("training score: %.2f" % cls.score(X_train, y_train))
    print("testing score: %.2f" % cls.score(X_test, y_test))

def test_MultinomialNB():
    # X_train, X_test, y_train, y_test = load_data()
    cls = naive_bayes.MultinomialNB()
    cls.fit(X_train, y_train)

    print("training score: %.2f" % cls.score(X_train, y_train))
    print("testing score: %.2f" % cls.score(X_test, y_test))

def test_MultinomialNB_alpha():
    alphas = np.logspace(-2, 5, num=20)
    train_score = []
    test_score = []

    for alpha in alphas:
        cls = naive_bayes.MultinomialNB(alpha=alpha)
        cls.fit(X_train, y_train)

        train_score.append(cls.score(X_train, y_train))
        test_score.append(cls.score(X_test, y_test))

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(alphas, train_score, label="training score")
    ax.plot(alphas, test_score, label="testing score")
    ax.set_xlabel(r'$\alpha$')
    ax.set_ylabel('score')
    # ax.set_ylim(0, 1.0)
    # todo: 为什么没有显示label
    ax.set_title("MultinomialNB")
    ax.set_xscale('log')
    plt.show()

if __name__ == "__main__":
    # test_GaussianNB()
    # test_MultinomialNB()
    test_MultinomialNB_alpha()