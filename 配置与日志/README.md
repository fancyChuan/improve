
## 配置

### configparser
使用python自带，类似于windows下的ini文件。官方文档：[https://docs.python.org/3/library/configparser.html](https://docs.python.org/3/library/configparser.html)

配置文件格式：
```
[DEFAULT]
ServerAliveInterval = 45
Compression = yes
CompressionLevel = 9
ForwardX11 = yes

[bitbucket.org]
User = hg

[topsecret.server.com]
Port = 50022
ForwardX11 = no
```

其中 [] 表示一个 section 单元，在python中以键值对存在，准确的说是：OrderedDict

#### 1. 写配置
按照字典操作 参见MainConfigTest.testWrite()

#### 2. 读配置