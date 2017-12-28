# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2017/12/21 15:30
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

def display(n):
    print '\n'.join([' '*(n-i-1) + '*'*(2*i+1) + ' '*(n-i-1) for i in range(n)])



if __name__ == '__main__':
    display(7)