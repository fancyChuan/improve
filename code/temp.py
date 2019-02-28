# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2018/5/24 10:23
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

import requests
import re


res = requests.get('http://www.ygdy8.net/html/gndy/china/index.html')
data = res.content
results = re.findall(r"option value='(.+)'.+option>", data)
print results
