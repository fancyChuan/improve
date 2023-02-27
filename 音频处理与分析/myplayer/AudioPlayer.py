import sys
import os
import wave

from PyQt5.QtCore import Qt, QTimer, QUrl
from PyQt5.QtGui import QIcon, QPixmap, QPainter, QPen
from PyQt5.QtMultimedia import QMediaContent, QMediaPlayer
from PyQt5.QtMultimediaWidgets import QVideoWidget
from PyQt5.QtWidgets import (QApplication, QMainWindow, QFileDialog,
                             QProgressBar, QPushButton, QHBoxLayout,
                             QVBoxLayout, QLabel, QWidget)


class AudioPlayer(QMainWindow):
    def __init__(self):
        super().__init__()

        # 初始化音频播放器
        self.player = QMediaPlayer(self)
        self.player.setVolume(50)

        # 初始化音频波形图
        self.waveform = Waveform()
        self.player.positionChanged.connect(self.waveform.update_position)

        # 初始化 UI
        self.init_ui()

    def init_ui(self):
        # 设置窗口标题和图标
        self.setWindowTitle('音乐播放器')
        self.setWindowIcon(QIcon('icon.png'))

        # 设置窗口尺寸
        self.resize(500, 300)

        # 创建文件打开按钮
        open_button = QPushButton('打开', self)
        open_button.clicked.connect(self.open_file)

        # 创建播放按钮
        play_button = QPushButton('播放', self)
        play_button.clicked.connect(self.play)

        # 创建暂停按钮
        pause_button = QPushButton('暂停', self)
        pause_button.clicked.connect(self.pause)

        # 创建水平布局，并将按钮添加到布局中
        button_layout = QHBoxLayout()
        button_layout.addWidget(open_button)
        button_layout.addWidget(play_button)
        button_layout.addWidget(pause_button)

        # 创建垂直布局，并将进度条、波形图和按钮布局添加到布局中
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.waveform)
        main_layout.addLayout(button_layout)

        # 创建主窗口中央的 QWidget，并设置其布局
        main_widget = QWidget(self)
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

    def open_file(self):
        # 打开文件选择对话框
        filename, _ = QFileDialog.getOpenFileName(
            self, '打开文件', os.path.expanduser('~'), '音频文件 (*.mp3 *.wav)')

        # 如果选择了文件，则设置音频播放器的媒体内容
        if filename:
            media = QMediaContent(QUrl.fromLocalFile(filename))
            self.player.setMedia(media)

    def play(self):
        # 如果音频播放器没有设置媒体内容，则不做任何操作
        if not self.player.media():
            return

        # 如果音频播放器已经暂停，则恢复播放
        if self.player.state() == QMediaPlayer.PausedState:
            self.player.play()
        else:
            self.player.setPosition(0)
            self.player.play()

    def pause(self):
        # 如果音频播放器正在播放，则暂停
        if self.player.state() == QMediaPlayer.PlayingState:
            self.player.pause()


class Waveform(QWidget):
    def __init__(self):
        super().__init__()

        # 初始化音频波形图的参数
        self.pen = QPen(Qt.darkGray, 1, Qt.SolidLine)
        self.brush = Qt.lightGray
        self.samples = []
        self.position = 0

        # 创建定时器，并连接到 update 方法上
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update)

        # 设置定时器的间隔为 20 毫秒
        self.timer.start(20)

    def update_position(self, position):
        self.position = position

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setPen(self.pen)

        # 计算音频波形图的宽度和高度
        width = self.width()
        height = self.height()

        # 绘制背景色
        painter.fillRect(0, 0, width, height, self.brush)

        # 绘制音频波形图
        if self.samples:
            max_sample = max(abs(sample) for sample in self.samples)
            if max_sample == 0:
                return
            scale = height / (2 * max_sample)
            center = height / 2
            step = int(len(self.samples) / width)
            for i in range(width):
                start = i * step
                end = min(start + step, len(self.samples))
                sample = sum(self.samples[start:end]) / (end - start)
                painter.drawLine(i, center - sample * scale,
                                  i, center + sample * scale)
        else:
            painter.drawText(event.rect(), Qt.AlignCenter, '没有音频数据')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    player = AudioPlayer()
    player.show()
    sys.exit(app.exec_())