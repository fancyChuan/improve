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

    def __getitem__(self, item):
        """
        允许像字典一样取值；同时，允许该类的实例可迭代，也允许反向迭代，比如
        > deck = FrenckDeck()
        > for d in deck:
        ...
        > for rd in reversed(deck):
        ...
        另外，当没有实现__contains__方法是，in操作也是调用此方法进行遍历
        >>> Card('Q', 'hearts') in deck
        True
        """
        return self._cards[item]

    def __len__(self):
        """
        这个特殊方法，运行对对象求长度。如：
        >>> deck = FrenckDeck()
        >>> len(deck)
        如果没有这个方法，上面的len(deck)会报错
        """
        return len(self._cards)

suit_values = dict(spades=3, hearts=2, diamonds=1, clubs=0)
def card_sort(card):
    index = FrenchDeck.ranks.index(card.rank)
    return index * len(suit_values) + suit_values[card.suit]


if __name__ == "__main__":
    deck = FrenchDeck()
    print len(deck)