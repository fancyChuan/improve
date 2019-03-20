# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/3/20 19:41
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import os
from configparser import ConfigParser

# 初始化
cp = ConfigParser()
cp.read("some_config.cfg")  # 任意后缀都可以


sections = cp.sections()

mysqld = cp.get('mysqld', 'host')
nothing = cp.get("what", "none")
