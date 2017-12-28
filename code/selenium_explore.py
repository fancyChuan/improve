# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2017/8/7 9:33
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import os


from selenium import webdriver
from selenium.webdriver.common.keys import Keys


# 初始化配置
def initWork():
    path = 'D:\Program Files (x86)\chromedriver.exe'
    os.environ['webdriver.chrome.driver'] = path
    driver = webdriver.Chrome(path)
    return driver


def initPhantomjs():
    path = 'D:/Program Files (x86)/phantomjs-2.1.1-windows/bin/phantomjs.exe'
    driver = webdriver.PhantomJS(executable_path=path)
    return driver



