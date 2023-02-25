import pygame
import os

pygame.init()

# 设置频谱图的宽度和高度
WIDTH = 640
HEIGHT = 480

# 设置音频的采样率、频道和位深度
pygame.mixer.pre_init(44100, -16, 2, 1024)

# 初始化Pygame
pygame.init()

# 设置屏幕大小和标题
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption('MP3 Player')


# 绘制频谱图
def draw_spectrum():
    # 获取当前播放时间和音频文件长度
    pos = pygame.mixer.music.get_pos()
    # length = pygame.mixer.music.get_busy()
    length = 10000

    # 检查音频是否正在播放
    if pos != -1:
        # 计算频谱图的高度
        height = int(HEIGHT * 0.5)
        width = int(pos / length * WIDTH)

        # 获取当前音量
        volume = pygame.mixer.music.get_volume()

        # 绘制频谱图
        pygame.draw.rect(screen, (0, 255, 0), (0, HEIGHT - height, width, height))
        pygame.draw.rect(screen, (255, 0, 0), (0, HEIGHT - int(volume * height), WIDTH, 1))


# 获取音频文件路径
audio_path = os.path.join(os.getcwd(), 'mp3/Ahxello-Infinity.mp3')

# 加载音频文件
pygame.mixer.music.load(audio_path)

# 播放音频
pygame.mixer.music.play()

# 主循环
while True:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            pygame.quit()
            quit()

    # 绘制频谱图
    draw_spectrum()

    # 更新屏幕
    pygame.display.update()
