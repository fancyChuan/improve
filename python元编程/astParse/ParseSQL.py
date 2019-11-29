# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/10/28 9:12
@Contact:  1247375074@qq.com 
@Software: PyCharm
需求分析：
    1. 连接数据库 connect_with(conf) 其中主要关注conf[db]拿到目标数据库
    2. 执行SQL的情况：
        2.1直接执行 rpt_db.query("select * from xxx")
        2.2先赋值后执行 sql = ""   rpt_db.query(sql)
            对于赋值操作，需要通过exec先执行，因为存在 "select %s, %s from xxx " % (x, y) 多个占位符
        2.3执行SQL拿到结果，再把结果集传到下一层SQL。 result_r = rpt_db.query(sql)   bi_db.batchInsert('tb_afterlend_urge_month', result_r)

    3. 需要动态执行的地方（选择性执行）：
        3.1 print语句
        3.2 非SQL赋值语句。比如 start_time = now()
        3.3 import 语句
        3.4 __main__中执行非py文件自定义函数的语句 today = datetime.datetime.today()
        3.5 queryResults(table, **args)

    4. 用于执行SQL的方法汇总
        4.1 query(sql) return res
        4.2 fetchone(sql) return res
        4.3 queryResults(table, **args) 这个函数下面的所有赋值语句都要执行，之后拼接成SQL
        4.4 insert(table, data, **args) data是一个字典，先从data拿到所有要插入的字段，然后拼接成 insert into table (f1,f2,f3...) values()
            这里解析的时候要拿到data对应的SQL，然后拼接成 insert into table (f1,f2,f3) select f1,f2,f3 from ...
        4.5 batchInsert(table, data) 同insert
        4.6 update(table,data,**args) 跟4.3的queryResults差不多，都在方法里面拼接
        4.7 updateBysql(sql)
        4.8 delete(table, **args) 同4.3
        4.9 querySql(sql,**agrs)
        4.10 query_data(sql)

    5. 全局语句也存在 db.query(sql) 和 x = db.query(sql) 两种类型

Module
    1. 先找到全局语句并执行
        1.1 找到import语句并执行
        1.2 Assign和Expr两种类型中，需要判断是否有执行sql

    2. 访问函数定义节点
        2.1 Assign和Expr两种类型中，需要判断是否有执行sql


MySQLdb执行返回格式

    cur.fetchall()     [(1,2,3,4), ...]
    cur.description  [a,b,c,d]  字段描述

    cur.fetchone()     [(1,2,3,4)]

20191107 引入新的功能，问题也随之变多，真的要慎重！



"""
