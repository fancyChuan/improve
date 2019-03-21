# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/3/20 19:58
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import configparser


def testWrite():
    """
    1. 写配置文件
        config 可以按照字典来使用，是一个拥有二级结构的字典
    """
    config = configparser.ConfigParser()

    config['DEFAULT'] = {"name": "fancyChuan", "age": 12}

    config['mysqld'] = {}
    config['mysqld']["host"] = "localhost"
    config['mysqld']["user"] = "fancyChuan"
    config['mysqld']["pass"] = "xx+yy=xx"

    with open("write.ini", "w") as fp:
        config.write(fp)


def testRead():
    """
    2. 读配置，跟dict的操作也一样
    """
    config = configparser.ConfigParser()
    config.read("read-config-sample.cfg")  # 任意后缀都可以
    print config.sections()
    print config.keys()

    # key/value中可以有多个值
    print(config["Multiline Values"]['chorus'])
    # 缩减
    print(config['Sections Can Be Indented'].keys())


if __name__ == "__main__":
    # testWrite()
    testRead()