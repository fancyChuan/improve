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

query = """update tb_compensate_detail_tmp a        
           set a.curDueMoney = case when a.loanType = '先息后本' then a.dueMonths*a.curShouldInterest 
           else a.dueMonths*(a.curShouldInterest+a.curShouldCapital) end,
          a.ReturnStatus = a.dueLevel  
          where a.dueDays>0 and a.statdate = '20191101'"""
# query = "select datacode from tb_dist_rpt where datatype = 'ContractTemplateType' " \
#         "and datacode in ('bzcd','bzcsd','ghzc','ghyc','sjshcd','bzmd','unifiedCarContractTemplate','unifiedCreditContractTemplate') " \
#         "UNION ALL  SELECT 'car_owner_credit'"

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

print("==============================================")
# select a from x 只有一个字段的时候，类型并不是 IdentifierList 这里需要增强
for token in query_tokens:
    if token.value.lower() == "from":
        break
    elif token.ttype is None and isinstance(token, sql.IdentifierList):
        for item in token.get_identifiers():
            print("------\n" + item.value)
            print(item.get_parent_name(), item.get_name(), item.get_alias(), item.get_real_name())
    elif token.ttype is None and isinstance(token, sql.Identifier):
        print(token.value)