# -*- encoding: utf-8 -*-
"""
@Author:   fancyChuan
@Time:     20201228
@Contact:  1247375074@qq.com
@Desc:     提取课程的视频文件名，同时统计视频时长
"""

import os
import pandas as pd

import os
import sys
import xlwt
from moviepy.editor import VideoFileClip


class FileCheck():

    def __init__(self):
        self.file_dir = ''

    def get_filesize(self, filename):
        u"""
        获取文件大小（M: 兆）
        """
        file_byte = os.path.getsize(filename)
        return self.sizeConvert(file_byte)

    def get_file_times(self, filename):
        u"""
        获取视频时长（s:秒）
        """
        clip = VideoFileClip(filename)
        file_time = self.timeConvert(clip.duration)
        return file_time

    def sizeConvert(self, size):  # 单位换算
        K, M, G = 1024, 1024 ** 2, 1024 ** 3
        if size >= G:
            return str(size / G) + 'G Bytes'
        elif size >= M:
            return str(size / M) + 'M Bytes'
        elif size >= K:
            return str(size / K) + 'K Bytes'
        else:
            return str(size) + 'Bytes'

    def timeConvert(self, size):  # 单位换算
        M, H = 60, 60 ** 2
        if size < M:
            return str(size) + u'秒'
        if size < H:
            return u'%s分钟%s秒' % (int(size / M), int(size % M))
        else:
            hour = int(size / H)
            mine = int(size % H / M)
            second = int(size % H % M)
            tim_srt = u'%s小时%s分钟%s秒' % (hour, mine, second)
            return tim_srt

    def get_all_file(self):
        u"""
        获取视频下所有的文件
        """
        for root, dirs, files in os.walk(file_dir):
            return files  # 当前路径下所有非目录子文件


i = 0
def find_video(itempath):
    result = []
    subfolders = os.listdir(itempath)
    for sub in subfolders:
        fullpath = itempath + u'/' + sub
        if os.path.isdir(itempath + '/' + sub):
            # print itempath + '/' + sub
            x = find_video(itempath + '/' + sub)
            result.extend(x)
            continue
        elif sub[-4:] in (u'.avi', u'.mp4', u'.wmv', u'.mov'):
            fc = FileCheck()
            t = fc.get_file_times(fullpath.encode('gbk'))
            result.append((sub, t))
            continue
        else:
            # print sub
            pass

    return result


if __name__ == '__main__':
    # print('\n'.join(find_video(u'M:/网络教程/尚硅谷大数据19年6月线下班/001_WEB')))
    data = []
    base = u"./尚硅谷大数据19年6月线下班/"
    folders = os.listdir(base)
    for item in folders:
        if "." not in item and '000JavaSe' not in item:
            video = find_video(base + item)
            data.extend([(item, x[0], x[1]) for x in video])

    df = pd.DataFrame(data, columns=['item', 'filename', 'times'])
    df.to_excel(u'课程明细.xlsx', encoding='gbk')
