import os
import sys
from pydub import AudioSegment
import pyaudio

# BASE_PATH = os.path.dirname(os.path.abspath(__file__)).decode(sys.getdefaultencoding())

# 打开MP3文件
audio = AudioSegment.from_file("mp3/Ahxello-Infinity.mp3")

# 初始化PyAudio
p = pyaudio.PyAudio()

# 打开音频输出流
stream = p.open(format=p.get_format_from_width(audio.sample_width),
                channels=audio.channels,
                rate=audio.frame_rate,
                output=True)

# 播放音频数据
data = audio.raw_data
while data:
    stream.write(data)
    data = audio.raw_data

# 停止音频输出流
stream.stop_stream()
stream.close()

# 关闭PyAudio
p.terminate()
