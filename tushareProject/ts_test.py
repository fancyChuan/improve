# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2018/5/21 15:11
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

import tushare as ts
import time

cons = ts.get_apis()

while 1:
    data = ts.get_today_all()
    print '----------'
    print 'shape of df: ', data.shape
    print data[data.code == '000023']
    time.sleep(2)
