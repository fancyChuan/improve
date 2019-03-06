# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/3/6 18:52
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
from __future__ import print_function
from hashlib import md5
from struct import unpack_from

ITEMS = 10000000
NODES = 100
NEW_NODES = 101

def test_nomal_hash():
    node_stat = [0 for i in range(NODES)]

    for item in range(ITEMS):
        k = md5(str(item)).digest()
        h = unpack_from(">I", k)[0]
        n = h % NODES
        node_stat[n] += 1

    _ave = ITEMS / NODES
    _max = max(node_stat)
    _min = min(node_stat)

    print("Ave: %d" % _ave)
    print("Max: %d\t(%0.2f%%)" % (_max, (_max - _ave) * 100.0 / _ave))
    print("Min: %d\t(%0.2f%%)" % (_min, (_ave - _min) * 100.0 / _ave))


def test_normal_hash_add():
    node = [0 for i in range(NODES)]

    change = 0

    for item in range(ITEMS):
        k = md5(str(item)).digest()
        h = unpack_from(">I", k)[0]
        n = h % NODES
        n_new = h % NEW_NODES
        if n_new != n:
            change += 1

    print("Change: %d\t(%0.2f%%)" % (change, change * 100.0 / ITEMS))

if __name__ == '__main__':
    # test_nomal_hash()
    test_normal_hash_add()