# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/2/28 17:17
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

from PIL import Image


def encodeDataToImage(image, data):
    evenImg = makeImageEven(image)  # 最低有效位为0的图片副本
    binary = ''.join(map(constLenBin, bytearray(data, "utf-8")))  # 将隐写的内容转为二进制字符序列
    if len(binary) > len(image.getdata()) * 3:
        raise Exception("Error: 图片太小，无法对待隐写数据完成隐写")

    encodedPixe = []
    for index, (r, g, b) in enumerate(evenImg.getdata()):
        if index * 3 < len(binary):
            # 这里对每个rgb的最后一个加上一个数值，0或者1，加上的这个数值就是隐写的内容
            newPixe = (r+int(binary[index*3+1]), g+int(binary[index*3+0]), b+int(binary[index*3+2]))
        else:
            newPixe = (r, g, b)
        encodedPixe.append(newPixe)
    encodedImg = Image.new(evenImg.mode, evenImg.size)
    encodedImg.putdata(encodedPixe)
    return encodedImg


def decodeDataFromImage(encodedImage):
    pixes = list(encodedImage.getdata())
    binary = ''.join([str(int(r>>1<<1 != r)) + str(int(g>>1<<1 != g)) + str(int(b>>1<<1 != b)) for r, g, b in pixes])
    # locationDoubleNull = binary.find('0' * 16)
    locationDoubleNull = binary.find('0000000000000000')
    # 判断该标识位是否是8位字节的终点
    endIndex = locationDoubleNull + (8 - locationDoubleNull % 8) if locationDoubleNull % 8 != 0 else locationDoubleNull
    data = binToString(binary[0: endIndex])


def makeImageEven(image):
    """
    将图片的RBG表示的每个元素的最后一位置为0 通过 先左移再右移 的方法
    """
    pixels = list(image.getdata())
    evenPixels = [(r >> 1 << 1, g >> 1 << 1, b >> 1 << 1) for r, g, b in pixels]
    newImg = Image.new(image.mode, image.size)
    newImg.putdata(evenPixels)
    return newImg


def constLenBin(i):
    """
    把十进制数字转为二进制字符串，并且不包含二进制标识 "0b"
    bin(3) -> '0b11'  需要转为 '00000011' 也就是8位表示
    """
    return "0" * (8 - (len(bin(i)) - 2)) + bin(i).replace("0b", "")


def binToString(binary):  # TODO：解码这部分没有调通
    index = 0
    string = []
    rec = lambda x, i: x[2:8] + (rec(x[8:], i - 1) if i > 1 else '') if x else ''
    # rec = lambda x, i: x and (x[2:8] + (i > 1 and rec(x[8:], i-1) or '')) or ''
    fun = lambda x, i: x[i + 1:8] + rec(x[8:], i - 1)
    while index + 1 < len(binary):
        chartype = binary[index:].index('0')  # 存放字符所占字节数，一个字节的字符会存为 0
        length = chartype * 8 if chartype else 8
        string.append(chr(int(fun(binary[index:index + length], chartype), 2)))
        index += length
    return ''.join(string)


if __name__ == "__main__":

    img = encodeDataToImage(Image.open("Desert.jpg"), "哈哈哈，这是我写入的~ if 666?")
    img.save("encodedImg.jpg")
    print(decodeDataFromImage(img))
