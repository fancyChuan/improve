# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2017/8/24 17:52
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('learnAngularJS.html')

if __name__ == '__main__':
    app.run()