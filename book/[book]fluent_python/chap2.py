# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2018/1/24 19:05
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

import collections

City = collections.namedtuple('City', "name country population location")
"""
namedtuple 的第二个参数是字段名，以空格隔开
"""
gz = City("gz", "china", 111, (33, 22))

shenzhen = City._make(["shenzhen", "china", 111, (33, 22)])
# _make第一个参数需要是可迭代的序列