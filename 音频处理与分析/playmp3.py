# -*- encoding: utf-8 -*-
"""
@Author:   infree
@Time:     2019/3/28 21:16
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import os
import sys
import time
import pygame
import wave
import pyaudio
import mp3play


BASE_PATH = os.path.dirname(os.path.abspath(__file__)).decode(sys.getdefaultencoding())

mp3_file = r"mp3\Ahxello-Infinity.mp3"


def usingPygame(filepath):
    pygame.mixer.init()
    pygame.mixer.music.load(filepath)
    pygame.mixer.music.play()  # 函数会立即返回，在后台播放音乐
    # 设定播放时间
    time.sleep(30)
    pygame.mixer.music.stop()


def usingPyaudio(filepath):
    chunk = 1024  # 2014kb
    wf = wave.open(filepath, 'rb')
    p = pyaudio.PyAudio()
    stream = p.open(format=p.get_format_from_width(wf.getsampwidth()), channels=wf.getnchannels(),
                    rate=wf.getframerate(), output=True)

    data = wf.readframes(chunk)  # 读取数据

    while True:
        data = wf.readframes(chunk)
        if data == "":
            break
        stream.write(data)
    stream.stop_stream()  # 停止数据流
    stream.close()
    p.terminate()  # 关闭 PyAudio
    print('play函数结束！')


def usingMp3Play(filepath): # 播放不了，报：指定的设备未打开，或不被 MCI 所识别的错误
    clip = mp3play.load(filepath)
    clip.play()
    duration = clip.milliseconds()  # 返回mp3文件共多少毫秒，注意这里的单位是毫秒

    time.sleep(duration)  # 现在就可以在后台播放完整的mp3了
    clip.stop()


if __name__ == "__main__":
    # usingPygame(mp3_file)
    # usingPyaudio(mp3_file)
    # usingMp3Play(r'E:\workshop\improve\audio\mp3\Ahxello-Infinity.mp3')
    usingMp3Play(BASE_PATH + "/" + mp3_file)
