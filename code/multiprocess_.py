# -*- encoding: utf-8 -*-
"""
Created on 4:54 PM 4/22/2017

@author: fancyChuan

多进程编程实践，使用multiprocessing模块
"""


# Python中的GIL限制了Python多线程并行对多核CPU的利用

import os
import multiprocessing as mtp

def run_proc(name):
    print 'run child process %s, pid: %s' % (name, os.getpid())


if __name__ == '__main__':
    print 'parent process, %s' % os.getpid()
    p = mtp.Process(target=run_proc, args=('what', ))
    p.start()
    p.join()
    print 'end ..'