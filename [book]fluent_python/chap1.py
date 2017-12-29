# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2017/12/29 10:01
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

import collections

Card = collections.namedtuple('Card', ['rank', 'suit'])

class FrenchDeck:
    ranks = [str(i) for i in range(2, 11)] + list('JQKA')
    suits = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank, suit) for rank in self.ranks for suit in self.suits]