
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
除了支持字典的操作，还有 config.sections() 可以获取所有的key，等同于 config.keys()

### 3. 数据转换
configparser 读取到的是string，需要自己转换成需要的数据类型

configparser 提供了一系列的getter方法： get(), getint(), getfloat(), getboolean()

bool()方法无法处理 "no/yes", "false/true"等数据，于是configparser提供了一个getboolean()方法，能够识别： 'yes'/'no', 'on'/'off', 'true'/'false' and '1'/'0' 
```
In[4]: bool("no")
Out[4]: True
In[5]: bool("false")
Out[5]: True

In[8]: conf = ConfigParser()
In[10]: conf['fancy'] = {}
In[11]: conf['fancy']['what'] = "no"
In[12]: conf['fancy']['f_or_t'] = "false"
In[13]: conf['fancy']['f_or_t'] = "true"
In[14]: conf['fancy']['f_or_t'] = "false"
In[15]: conf['fancy']['f_or_t2'] = "true"
In[16]: conf.get("fancy", 'f_or_t')
Out[16]: u'false'
In[17]: conf.getboolean("fancy", 'f_or_t')
Out[17]: False
In[18]: conf.getboolean("fancy", 'f_or_t2')
Out[18]: True
In[19]: conf.getboolean("fancy", 'what')
Out[19]: False

```
### 4. 支持设置默认值
dict.get() 可以在查询不到key的时候可以提供默认值，在configparser里面就可以
```
In[24]: fancy = conf['fancy']
In[25]: fancy.get('you') # 获取不到这个key
In[26]: fancy.get('you', "who") # 设置了默认值
Out[26]: 'who'
# 对于ConfigParser对象，需要加一个fallback参数，显示指明默认值
In[30]: conf.get("fancy", "some", fallback="hahaha")
Out[30]: 'hahaha'
```

### 5. 支持INI文件结构
参见  read-config-sample.cfg 样例
- [section] 一个配置单元
- key/value 默认使用 = ; 作为分隔符
- 行的开头是 # 或者 ; 表示注释
- 支持缩进
- 可以不提供key的value，但是分隔符还是需要的
