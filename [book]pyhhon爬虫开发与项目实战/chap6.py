# -*- encoding: utf-8 -*-
"""
Created on 11:01 PM 12/27/2017

@author: fancyChuan
"""


class UrlManager(object):
    def __init__(self):
        self.new_urls = set()
        self.old_urls = set()

    def newUrlSize(self):
        return len(self.new_urls)

    def hasNewUrl(self):
        return self.newUrlSize() != 0

    def addNewUrl(self, url):
        if url is None: # 要有意识判断变量是否为空,set的元素可以是空，在这里url不能为空
            pass
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.add(url)

    def addNewUrls(self, urls):
        self.new_urls