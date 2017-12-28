# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2017/8/24 10:23
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""

from turtle import *
import time
import turtle


def go_to(x, y):
   up()
   goto(x, y)
   down()


def big_Circle(size):  #函数用于绘制心的大圆
   speed(0)
   for i in range(150):
       forward(size)
       right(0.3)

def small_Circle(size):  #函数用于绘制心的小圆
    speed(0)
    for i in range(210):
        forward(size)
        right(0.786)

def line(size):
    speed(1)
    forward(51*size)

def heart( x, y, size):
    go_to(x, y)
    left(150)
    begin_fill()
    line(size)
    big_Circle(size)
    small_Circle(size)
    left(120)
    small_Circle(size)
    big_Circle(size)
    line(size)
    end_fill()

def arrow():
    pensize(10)
    setheading(0)
    go_to(-400, 0)
    left(15)
    forward(300)
    go_to(269, 178)
    forward(150)

def arrowHead():
    pensize(1)
    speed(1)
    color('red', 'red')
    begin_fill()
    left(120)
    forward(20)
    right(150)
    forward(35)
    right(120)
    forward(35)
    right(150)
    forward(20)
    end_fill()


def main():
    start = time.time()
    pensize(2)
    color('red', 'pink')
    #getscreen().tracer(30, 0) #取消注释后，快速显示图案
    heart(130, 0, 1)          #画出第一颗心，前面两个参数控制心的位置，函数最后一个参数可控制心的大小
    setheading(0)             #使画笔的方向朝向x轴正方向
    heart(-80, -100, 1.5)     #画出第二颗心
    arrow()                   #画出穿过两颗心的直线
    arrowHead()               #画出箭的箭头
    go_to(200, -200)
    write("小仙女要乖哟", move=False, align="center", font=("宋体", 20, "normal"))
    go_to(260, -240)
    write("七夕快乐！", move=False, align="center", font=("宋体", 20, "normal"))
    print 'time:', time.time() - start
    done()

def another():
    import turtle
    import math
    wn = turtle.Screen()
    wn.setworldcoordinates(-2, -2, 2, 2)
    alex = turtle.Turtle()
    alex.color("red")
    alex.pensize(2)
    alex.penup()
    alex.speed(0)
    walkStart = -1
    walkEnd = 1
    i = walkStart
    j = walkEnd
    while i <= 0 and j >= 0:
        y1 = math.sqrt(1 - i * i) + (i * i) ** (1 / 3.0)
        y2 = -math.sqrt(1 - i * i) + (i * i) ** (1 / 3.0)
        y3 = math.sqrt(1 - j * j) + (j * j) ** (1 / 3.0)
        y4 = -math.sqrt(1 - j * j) + (j * j) ** (1 / 3.0)
        alex.setx(i)
        alex.sety(y1)
        alex.dot()
        alex.sety(y2)
        alex.dot()
        alex.setx(j)
        alex.sety(y3)
        alex.dot()
        alex.sety(y4)
        alex.dot()
        i += 0.01
        j -= 0.01
    wn.exitonclick()

main()
