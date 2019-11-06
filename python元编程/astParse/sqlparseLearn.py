# -*- encoding: utf-8 -*-
"""
@Author:   ZhangXieChuan
@Time:     2019/11/6 15:41
@Contact:  1247375074@qq.com 
@Software: PyCharm
"""
import sqlparse
from sqlparse import sql

query = "select id as myid ,r.fname, r.lname as mylname, address, max(a.total_delayint) - sum(a.actint)  xxx \
         from res_users as r left join res_partner as p on p.id=r.partner_id \
         where name = (select name from res_partner where id = 1)"

query_tokens = sqlparse.parse(query)[0].tokens
for token in query_tokens:
    if token.ttype is None and isinstance(token, sql.IdentifierList):
        print('Identifierlist:' + ";".join([x.value for x in token.get_identifiers()]))
        for item in token.get_identifiers():
            # (u'r.lname as mylname'      u'mylname'     u'mylname'      u'lname')
            # item.value  原SQL中的表达式 即 r.lname as mylname
            # get_name() 和 get_alias() 差不多，都是最后生效的字段名。get_name一定有值，如果没有用as的话，那么get_alias为None
            # get_real_name() 原表字段名
            # get_parent_name() 表名
            print("------\n" + item.value)
            print(item.get_parent_name(), item.get_name(), item.get_alias(), item.get_real_name())

