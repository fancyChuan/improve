# coding=utf-8
import logging
import datetime
import sys
from com.touna.db import db_connection
from com.touna.common.db_common import *
from com.touna.util.exec_script import *
from itertools import product
import traceback
import collections
from decimal import Decimal


# 员工信息表转化
def trans_emp():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]员工信息表tb_emp_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_emp_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_emp_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]员工信息表tb_emp_rpt->trytimes:', trytimes
        try:
            clear_table('tb_emp_rpt_temp')
            sql = "INSERT INTO tb_emp_rpt_temp \
                       (ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                       POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                       FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,LOGINNAME,AUDITLEVEL)\
                SELECT a.ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                       POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                       FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,b.USERNAME,AUDITLEVEL\
                  FROM tio_xd_sys_emp a \
                  LEFT JOIN tio_xd_sys_user b ON b.EMPID=a.ID \
                  LEFT JOIN tio_xd_lb_audituserset c ON c.USERNAME=b.USERNAME \
                 GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_emp_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]员工信息表tb_emp_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_emp_rpt')
                sql_db = "insert into tb_emp_rpt  \
                            (ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                             POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                             FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,LOGINNAME,AUDITLEVEL) \
                      select ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                             POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                             FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,LOGINNAME,AUDITLEVEL \
                        from tb_emp_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

                print "sys_emp更新开始时间: %s" % (start_time)
                sql = 'insert into tb_sys_emp(EMP_NO,EMP_NAME,ID_CARD,GENDER,DUTIES,ORG_ID) SELECT a.EMPNO,a.EMPNAME,a.IDCARD,a.GENDER,a.DUTIES,a.ORGID\
                    		      FROM tb_emp_rpt a where not EXISTS (SELECT b.EMP_NO FROM tb_sys_emp b where a.EMPNO=b.emp_no ) '
                r = rpt_db.query(sql)
                rpt_db.commit()
                end_time = datetime.datetime.today()
                print "sys_emp更新结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
                # 更新所属机构ID
                sql = """UPDATE tb_sys_emp s,tb_emp_rpt t SET s.org_id=t.orgid,s.EMP_NAME=t.EMPNAME WHERE s.emp_no=t.empno"""
                r = rpt_db.query(sql)
                rpt_db.commit()

                # 特殊处理郑磊对应组织  项奕敏、张继娥、盛晗
                sql = "update tb_sys_emp a set a.org_id = 1059 where a.EMP_NO in('WJ10848','WJ69833','WJ30046','WJ51123','WJ10065','WJ10668')"
                r = rpt_db.query(sql)
                rpt_db.commit()

                # 付磊帐号特殊处理
                sql = "update tb_sys_emp a set a.org_id = 1130 where a.EMP_NO ='WJ54468'"
                r = rpt_db.query(sql)
                rpt_db.commit()

                # 谢颖帐号、隋涵博、王筱舟特殊处理成催收部门
                sql = "update tb_sys_emp a set a.org_id = 1064 where a.EMP_NO IN ('WJ35853','WJ67101','WJ56093')"
                rpt_db.updateBysql(sql)

                # 冻结离职员工账号
                sql = "update  tb_sys_user a , tb_emp_rpt b set a.USER_STATUS=0 where a.EMP_NO=b.EMPNO and b.LEAVESTATUS=1 and a.USER_STATUS=1 and a.`STATUS`=1 and a.EMP_NO<>'WJ35853';"
                r = rpt_db.query(sql)
                rpt_db.commit()

                # 冻结离职员工账号(HR系统员工信息表)
                sql = "update tb_sys_user a , tio_hr_sys_emp b set a.USER_STATUS=0 \
                     where a.EMP_NO=b.workcode and (b.status=5 or b.leaveDate is not null) and a.USER_STATUS=1 and a.`STATUS`=1 \
                       and b.statdate = date_format(DATE_SUB(CURDATE(), INTERVAL 1 DAY),'%Y%m') and a.EMP_NO <> 'WJ35853';"
                r = rpt_db.query(sql)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]员工信息表tb_emp_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 员工信息历史表转化
def trans_emp_his():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]员工信息历史表tb_emp_his_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_emp_his_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_emp_his_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]员工信息历史表tb_emp_his_rpt->trytimes:', trytimes
        try:
            clear_table('tb_emp_his_rpt_temp')
            sql = "INSERT INTO tb_emp_his_rpt_temp \
                      (ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                       POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                       FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,STARTTIME,ENDTIME) \
                SELECT a.ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,HOMEADDR,\
                       POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,\
                       FULLMEMBERTIME,HIGHEROFFICETIME,MOVETIME,STARTTIME,ENDTIME\
                  FROM tio_xd_sys_emp_his a \
                  LEFT JOIN tio_xd_sys_user b ON b.EMPID=a.ID "
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_emp_his_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['cnt'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]员工信息历史表tb_emp_his_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_emp_his_rpt')
                sql_db = "insert into tb_emp_his_rpt  \
                            (ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,\
                            HOMEADDR,POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,CREATEBY,CREATETIME,MODIFYBY,\
                            MODIFYTIME,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,FULLMEMBERTIME,\
                            HIGHEROFFICETIME,MOVETIME,starttime,endtime,LOGINNAME,AUDITLEVEL) \
                      select ID,EMPNO,EMPNAME,IDCARD,GENDER,BIRTHDAY,DUTIES,ORGID,TEL,MOBILE,HOMETEL,OFFICETEL,\
                            HOMEADDR,POST,EMAIL,ISSALESMAN,ISSUPERVISER,REMARK,CREATEBY,CREATETIME,MODIFYBY,\
                            MODIFYTIME,JOBDATE,LEAVESTATUS,LEAVEDATE,VACATION,ISDIRECT,FULLMEMBERTIME,\
                            HIGHEROFFICETIME,MOVETIME,starttime,endtime,LOGINNAME,AUDITLEVEL\
                        from tb_emp_his_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

                # 取每个工号历史表中第一条记录（逾期、放款、进度表 蚂蚁贷产品 直销判断用）
                clear_table('tb_emp_his_first_temp')
                sql = """INSERT INTO tb_emp_his_first_temp(id,ISDIRECT,ORGID)
                    SELECT a.id, a.ISDIRECT, a.ORGID
                      FROM tb_emp_his_rpt    a
                     INNER JOIN (SELECT id, MIN(starttime) starttime
                      FROM tb_emp_his_rpt GROUP BY id) b
                        ON a.ID = b.id AND a.starttime = b.starttime GROUP BY a.id"""
                r = rpt_db.query(sql)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]员工信息历史表tb_emp_his_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 组织机构表转化
def trans_org():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]组织机构信息表tb_org_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_org_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_org_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]组织机构信息表tb_org_rpt->trytimes:', trytimes
        try:
            clear_table('tb_org_rpt_temp')
            sql = "INSERT INTO tb_org_rpt_temp \
                      (ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,MODIFYBY,MODIFYTIME,STATUS,micro_store_flag) \
               select ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,MODIFYBY,MODIFYTIME,STATUS,micro_store_flag\
                 from tio_xd_sys_org "
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_org_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]组织机构信息表tb_org_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_org_rpt')
                sql_db = "insert into tb_org_rpt  \
                            (ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,MODIFYBY,MODIFYTIME,STATUS,micro_store_flag) \
                      select ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,MODIFYBY,MODIFYTIME,STATUS,micro_store_flag\
                        from tb_org_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]组织机构信息表tb_org_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 组织机构历史表转化
def trans_org_his():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]组织机构信息历史表tb_org_his_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_org_his_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_org_his_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]组织机构信息表tb_org_his_rpt->trytimes:', trytimes
        try:
            clear_table('tb_org_his_rpt_temp')
            sql = "INSERT INTO tb_org_his_rpt_temp \
                      (ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,STARTTIME,ENDTIME) \
               select ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,STARTTIME,ENDTIME\
                 from tio_xd_sys_org_his "
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_org_his_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]组织机构信息表tb_org_his_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_org_his_rpt')
                sql_db = "insert into tb_org_his_rpt  \
                            (ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,STARTTIME,ENDTIME) \
                      select ID,ORGNO,ORGNAME,ORGLEADER,PID,ISSITE,ISAUDIT,ORGLEADERID,SITECODE,STARTTIME,ENDTIME\
                        from tb_org_his_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]组织机构信息历史表tb_org_his_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 区域表转化
def trans_area():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]区域表tb_area开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '区域表tb_area重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '区域表tb_area重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]区域表tb_area->trytimes:', trytimes
        try:
            clear_table('tb_area_temp')
            # 20180704 change  第二大区廖庆福区域 无效，不记录进来
            sql = "INSERT INTO tb_area_temp \
                     (ID,AREANO,AREANAME,AREALEADER,PID,AREALEADERID,AREALEADEREMPNO,ISSITE,AREATYPE,\
                      CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME) \
               SELECT DISTINCT \
                      IF(o.issite=1,o.Pid,o.id) ID ,\
                      IF(o.issite=1,so.bigareaNO,o.orgNo) AREANO,\
                      IF(o.issite=1,so.bigareaname,o.orgName) AREANAME,\
                      IF(o.issite=1,so.bigareaLEADER,o.orgleader) AREALEADER,\
                      IF(o.issite=1,so.id,o.PID) PID,\
                      IF(o.issite=1,so.bigareaLEADERID,o.orgleaderid) AREALEADERID,\
                      IF(o.issite=1,so.bigareaderempno,e.empno) AREALEADEREMPNO,\
                      IF(o.issite=1,so.issite,o.issite) issite,\
                      bigtype AREATYPE,\
                      'admin',now(),'admin',now()\
                FROM tb_bigarea so \
                LEFT JOIN tb_org_rpt o ON o.pid=so.id AND o.status = 1 AND o.issite!=-1 \
                LEFT JOIN tb_emp_rpt e ON o.orgLEADERID=e.id\
                WHERE o.id IS NOT NULL AND o.issite !=-1 and o.id <>1316"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_area_temp"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]区域表tb_area_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_area')
                sql_db = "insert into tb_area  \
                             (ID,AREANO,AREANAME,AREALEADER,PID,AREALEADERID,AREALEADEREMPNO,\
                              ISSITE,AREATYPE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME) \
                       select ID,AREANO,AREANAME,AREALEADER,PID,AREALEADERID,AREALEADEREMPNO,\
                              ISSITE,AREATYPE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME\
                         from tb_area_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]区域表tb_area结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True

# 20181227 change  袋吧事业部剔除渠道部
# 大区表转化
def trans_bigarea():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]大区表tb_bigarea开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '大区表tb_bigarea重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '大区表tb_bigarea重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]大区表tb_bigarea->trytimes:', trytimes
        try:
            clear_table('tb_bigarea_temp')
            sql = """INSERT INTO tb_bigarea_temp
                     (id,BIGAREANO,BIGAREANAME,BIGAREALEADER,PID,BIGAREALEADERID,BIGAREADEREMPNO,
                      ISSITE,BIGTYPE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME)
               select o.ID,ORGNO,ORGNAME,ORGLEADER,PID,ORGLEADERID,e.empno,o.issite,1,'admin',now(),'admin',now()
                 from tb_org_rpt o
                 left join tb_emp_rpt e on o.ORGLEADERID=e.id
                where (issite is null or issite!=1) and o.status = 1 and pid = 1488
                UNION ALL
                select o.ID,ORGNO,ORGNAME,ORGLEADER,PID,ORGLEADERID,e.empno,o.issite,2,'admin',now(),'admin',now()
                  from tb_org_rpt o
                  left join tb_emp_rpt e on o.ORGLEADERID=e.id
                 where o.status=1 and o.pid = 1985 and o.orgname not like '%渠道部%' """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_bigarea_temp limit 100"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]区域表tb_bigarea_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_bigarea')
                sql_db = "insert into tb_bigarea  \
                            (id,BIGAREANO,BIGAREANAME,BIGAREALEADER,PID,BIGAREALEADERID,BIGAREADEREMPNO,\
                             ISSITE,BIGTYPE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME) \
                      select id,BIGAREANO,BIGAREANAME,BIGAREALEADER,PID,BIGAREALEADERID,BIGAREADEREMPNO,\
                             ISSITE,BIGTYPE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME \
                        from tb_bigarea_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]大区表tb_bigarea结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 省级经理转化
def trans_provincemanage():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]省级经理tb_provincemanage开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '省级经理tb_provincemanage重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '省级经理tb_provincemanage重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]省级经理表tb_provincemanage->trytimes:', trytimes
        try:
            clear_table('tb_provincemanage_temp')

            sql = "INSERT INTO tb_provincemanage_temp \
                       (ID,PROMANANO, PROMANA, PROMANALEADER,SUPERIORID,AreaID, PROMANALEADERID,PROMANALEADERNO,\
                        ISSITE,type,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME,data_status)\
                SELECT IF(org.issite=1,area.id,org.id ) ID, \
                        IF(org.issite=1,area.AREANO,orgno) PROMANANO, \
                        IF(org.issite=1,area.AREANAME,orgName) PROMANA, \
                        IF(org.issite=1,area.AREALEADER,orgleader) PROMANALEADER, \
                        IF(org.issite=1,area.PID,org.pid) SUPERIORID, \
                        area.id AreaID, \
                        IF(org.issite=1,area.AREALEADERID,orgleaderid) PROMANALEADERID, \
                        IF(org.issite=1,area.AREALEADEREMPNO,emp.empno) PROMANALEADERNO, \
                        IF(org.issite=1,area.issite,org.issite) issite, \
                        area.areatype TYPE,\
                        'admin',now(),'admin',now(),1 \
                   FROM tb_area AREA \
                   LEFT JOIN tb_bigarea big ON area.id =big.id \
                   LEFT JOIN tb_org_rpt org ON area.ID =org.PID and org.status = 1 \
                   LEFT JOIN tb_emp_rpt emp ON org.orgleaderid=emp.id \
                  WHERE big.id IS NULL and org.issite !=-1\
                  GROUP BY id\
                  UNION ALL\
                 SELECT area.id ID, \
                        area.AREANO PROMANANO, \
                        area.AREANAME PROMANA, \
                        area.AREALEADER PROMANALEADER, \
                        area.PID SUPERIORID, \
                        area.id AreaID, \
                        area.AREALEADERID PROMANALEADERID, \
                        area.AREALEADEREMPNO PROMANALEADERNO,\
                        area.issite issite,\
                        area.areatype TYPE,\
                        'admin',now(),'admin',now(),1 \
                   FROM tb_area AREA \
                   LEFT JOIN tb_bigarea big ON area.id =big.id \
                  WHERE big.id IS NOT NULL\
                  GROUP BY id"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tb_provincemanage_temp limit 100"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]省级经理tb_provincemanage_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_provincemanage')
                sql_db = "insert into tb_provincemanage  \
                              (ID,PROMANANO,PROMANA,PROMANALEADER,SUPERIORID,PROMANALEADERID,PROMANALEADERNO,TYPE,\
                              ISSITE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME,data_status,AreaID) \
                       select ID,PROMANANO,PROMANA,PROMANALEADER,SUPERIORID,PROMANALEADERID,PROMANALEADERNO,TYPE,\
                              ISSITE,CREATE_USER,CREATE_TIME,UPDATE_USER,UPDATE_TIME,data_status,AreaID \
                         from tb_provincemanage_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]省级经理tb_provincemanage结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 分公司表转化
def trans_site():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]分公司表tb_site_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_site_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_site_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]分公司表tb_site_rpt->trytimes:', trytimes
        try:
            clear_table('tb_site_rpt_temp')
            sql = "INSERT INTO tb_site_rpt_temp \
                       (ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,areaname,\
                        opendate,siteallname,PROVINCE,CITY,CITYZONE,YONGYOU_CODE,DEPARTMENT)\
                SELECT a.ID,SITECODE,SITENAME,SITEADDR,SITETEL,a.STATUS,ORGID,a.AREACODE,b.DATAVALUE,\
                       DATE_FORMAT(OPENDATE, '%Y-%m-%d') AS OPENDATE,siteallname,PROVINCE,CITY,CITYZONE,YONGYOU_CODE,DEPARTMENT \
                  FROM tio_xd_sys_site a \
                  LEFT JOIN tio_xd_sys_dist b ON a.AreaCode = b.DATACODE AND b.DATATYPE = 'AreaCode'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 删除广州金宝盘
            sql = "delete from tb_site_rpt_temp where sitecode = '010'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 更新cityno为空的数据 -处理非市辖区的情况
            sql = "UPDATE tb_site_rpt_temp a ,tio_xd_sys_city b, tb_province_city c \
                    SET a.cityno = c.cityno \
                  WHERE a.city = b.cityid \
                    AND b.cityname = c.cityname \
                    AND (a.cityno IS NULL OR a.cityno ='') "
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 更新cityno为空的数据 -处理市辖区的情况
            sql = "UPDATE tb_site_rpt_temp a , \
                       (SELECT provinceid,MIN(cityno)cityno FROM tb_province_city GROUP BY provinceid ) c \
                   SET a.cityno = c.cityno \
                 WHERE CAST(a.province AS SIGNED) = c.provinceid \
                   AND (a.cityno IS NULL)"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 修改分公司对应的区域ID,以及省级经理id
            sql = "update tb_site_rpt_temp s,\
                        (select areaId,sitecode,a.id ,case when a.type=1 then 'gd' when a.type = 2 then 'db' end type\
                           FROM tb_provincemanage a \
                           LEFT JOIN tb_org_rpt b ON a.id=b.PID and b.issite=1\
                          where SITECODE!='' \
                          GROUP BY SITECODE) a\
                    SET s.areaid=a.areaid,s.superiorId=a.id,s.department = ifnull(a.type,s.department)\
                  where s.sitecode=a.sitecode"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 修改分公司对应的区域，以及省级经理（省级经理就是分公司的，比如直销部）
            sql = "update tb_site_rpt_temp s,\
                       (select areaId,sitecode,a.id,case when a.type=1 then 'gd' when a.type = 2 then 'db' end  type \
                          FROM tb_provincemanage a \
                          LEFT JOIN tb_org_rpt b ON a.id=b.ID and b.issite=1\
                         where a.issite=1\
                         GROUP BY SITECODE )a\
                   SET s.areaid=a.areaid,s.superiorId=a.id,s.department = ifnull(a.type,s.department) \
                 where s.sitecode=a.sitecode"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select sitecode from tb_site_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['sitecode'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]分公司表tb_site_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_site_rpt')
                sql_db = "insert into tb_site_rpt  \
                            (ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,AREANAME,opendate,siteallname,\
                             PROVINCE,CITY,CITYZONE,YONGYOU_CODE,CITYNO,areaid,SUPERIORID,DEPARTMENT) \
                      select ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,AREANAME,opendate,siteallname,\
                             PROVINCE,CITY,CITYZONE,YONGYOU_CODE,CITYNO,areaid,SUPERIORID,DEPARTMENT \
                        from tb_site_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]分公司表tb_site_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 分公司历史记录tb_site_his_rpt
def trans_site_his():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]分公司历史记录tb_site_his_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '分公司历史记录tb_site_his_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '分公司历史记录tb_site_his_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]分公司历史记录tb_site_his_rpt->trytimes:', trytimes
        try:
            clear_table('tb_site_his_rpt_temp')
            sql = "insert into tb_site_his_rpt_temp  \
                       (ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,AREANAME,STARTTIME,ENDTIME) \
                select a.ID,SITECODE,SITENAME,SITEADDR,SITETEL,a.STATUS,ORGID,AREACODE,b.DATAVALUE,a.STARTTIME,a.ENDTIME\
                  from tio_xd_sys_site_his a \
                  left join tio_xd_sys_dist b on a.AreaCode = b.DATACODE AND b.DATATYPE = 'AreaCode'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_site_his_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]分公司历史记录tb_site_his_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_site_his_rpt')
                sql_db = "insert into tb_site_his_rpt  \
                             (ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,AREANAME,STARTTIME,ENDTIME) \
                      select ID,SITECODE,SITENAME,SITEADDR,SITETEL,STATUS,ORGID,AREACODE,AREANAME,STARTTIME,ENDTIME\
                        from tb_site_his_rpt_temp A "
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]分公司历史记录tb_site_his_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 贷款申请表转化
def trans_applyinfo():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库
    statdate = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m')

    start_time = datetime.datetime.today()
    print "[基础数据转化]贷款申请表tb_applyinfo_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '贷款申请表tb_applyinfo_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '贷款申请表tb_applyinfo_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]贷款申请表tb_applyinfo_rpt->trytimes:', trytimes
        try:

            # 车辆评估表 （获取车牌号）
            clear_table('tbl_lb_evaluatereport_temp')
            sql = """INSERT INTO tbl_lb_evaluatereport_temp(applyid,carNO,carBrand,regDate,ENGINENO,CARTYPE,DRIVENO)
                    SELECT a.APPLYID,a.carno,a.BRAND CARBRAND,a.regDate,a.ENGINENO,a.CARTYPE,a.DRIVENO
                     FROM tio_xd_lb_evaluatereport a ,
                                (SELECT applyid,MAX(update_time) update_time
                                        FROM tio_xd_lb_evaluatereport
                                 WHERE CARNO IS NOT NULL
                                GROUP BY APPLYID) b
                    WHERE a.applyid = b.applyid AND a.update_time=b.update_time
                        AND a.CARNO IS NOT NULL
                    GROUP BY a.APPLYID"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            clear_table('tb_applyinfo_rpt_temp')
            # 20181221 change  小贷申请表中直销坐席 字段如果是乐高经纪人进件的，存的是经纪人名字，不是坐席，需处理掉
            sql = "INSERT INTO tb_applyinfo_rpt_temp \
                      (APPLYID,productarea,SITENO,SITENAME,PRODUCTID,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTID,CUSTNAME,DOCNO,\
                       ORGID,ORGNAME,ORGLEADERID,ORGLEADER,LASTAUDITRESULT,LASTAUDITTIME,LOANTYPE,\
                       SALESMAN,EMPNAME,AREACODENO,AREACODENAME,PRODUCTTYPE,ISSITE,ISAUDIT,FULLMEMBERTIME,HIGHEROFFICETIME,\
                       MOVETIME,HKFS,DKFS,EMPNO,CARNO,CARBRAND,FCDZ,FCZH,ASSPRICE,STATUS,\
                       SURVEYMAN,SURVEYTIME,SURVEYSTATUS,SURVEYUSER,MSMANID,MSMANNAME,MSTIME,CREATEBY,HKLX,SITELEADERID,\
                       SITELEADER,PGJ,DEPOSIT,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,DIRECTSELLER,CREATETIME,PHONECHECKUSER,\
                       LOANTIME,DIRECT_EMPID,applycode,single_Premiums,inviterID,inviterCode,inviterName,inviterDuties,\
                       agentID,agentName,agentCode,identify,PURPOSE,orgno,last_apply_id,reloan_flag,reloan_type,manage_fee_flag,loan_sources,\
                       inviter_empid,inviter_empcode,inviter_empname,inviter_empduty)\
                SELECT a.ID,a.productarea ,a.SITENO,b.SITENAME,c.ID,c.PRODUCTNAME,c.CONTRACTTEMPTYPE,d.DATAVALUE,e.ID,e.CUSTNAME,e.DOCNO, \
                       f.ID,f.ORGNAME,f.ORGLEADERID,f.ORGLEADER,g.LASTAUDITRESULT,DATE_FORMAT(g.LASTAUDITTIME, '%Y-%m-%d'),a.LOANTYPE,\
                       a.SALESMAN,h.EMPNAME,b.AreaCode,j.DATAVALUE,c.PRODUCTTYPE,f.ISSITE,f.ISAUDIT,h.FULLMEMBERTIME,h.HIGHEROFFICETIME,\
                       h.MOVETIME,a.HKFS,a.DKFS,h.EMPNO,ifnull(i2.CARNO,i.CARNO),ifnull(i2.BRAND,i.CARBRAND),i.FCDZ,i.FCZH,\
                       if(a.ASSPRICE is NULL,0,a.ASSPRICE),a.STATUS, \
                       k.SURVEYMAN,DATE_FORMAT(k.SURVEYTIME, '%Y-%m-%d')SURVEYTIME,k.STATUS,kk.USERNAME,l.SALESMAN,l.SALESMAN_STR,l.MSTIME,a.CREATEBY,a.HKLX,n.ORGLEADERID,\
                       n.ORGLEADER,a.PGJ,taa.DEPOSIT ,a.SOURCES,a.AUDITTIME,a.AMOUNT,DATE_FORMAT(a.APPLYDATE, '%Y-%m-%d')APPLYDATE,\
                       case when q.id is null OR a.DIRECT_EMPID IS NOT NULL then a.DIRECTSELLER end DIRECTSELLER,a.CREATETIME,pc.real_name,\
                       a.LOANTIME,a.DIRECT_EMPID,a.applycode,a.single_Premiums,\
                       q.inviterID,q.inviterCode,q.inviterName,q.inviterDuties,\
                       q.id agentID,q.real_name agentName,q.agentCode,r.identify,a.PURPOSE,\
                       a.orgno,a.last_apply_id,a.reloan_flag,a.reloan_type,a.manage_fee_flag,a.loan_sources,\
                       q.inviter_empid,q.inviter_empcode,q.inviter_empname,q.inviter_empduty\
                  FROM tio_xd_lb_applyinfo a\
                  LEFT JOIN tio_xd_sys_site_his b ON (a.siteno = b.sitecode AND b.starttime<a.CREATETIME AND a.CREATETIME<=b.endtime)\
                  LEFT JOIN tio_xd_sys_product c ON a.PRODUCTS = c.ID\
                  LEFT JOIN tio_xd_sys_dist d ON c.CONTRACTTEMPTYPE = d.DATACODE AND d.DATATYPE = 'ContractTemplateType' \
                  LEFT JOIN tio_xd_sys_emp_his h ON (a.SALESMAN = h.ID AND h.starttime<a.CREATETIME AND a.CREATETIME<=h.endtime AND a.SALESMAN>='0' and substr(a.SALESMAN,1,1) in('1','2','3','4','5','6','7','8','9'))\
                  LEFT JOIN tio_xd_lb_custinfo e ON a.ID = e.APPLYID\
                  LEFT JOIN tio_xd_sys_org_his f ON (h.orgid = f.id AND f.starttime<a.CREATETIME AND a.CREATETIME<=f.endtime AND H.ORGID >0)\
                  LEFT JOIN tio_xd_lb_applyaudit_last g ON a.ID = g.APPLYID\
                  LEFT JOIN tio_xd_sys_dist j ON b.AreaCode = j.DATACODE AND j.DATATYPE = 'AreaCode'\
                  LEFT JOIN tio_xd_lm_dzyw i ON a.ID = i.APPLYID\
                  LEFT JOIN tio_xd_lb_carmessage i2 ON e.id = i2.custid\
                  LEFT JOIN tio_xd_lb_survey k ON a.ID = k.APPLYID\
                  LEFT JOIN tio_xd_sys_phone_check_user pc ON pc.user_name = k.phone_check_usr\
                  LEFT JOIN tio_xd_lb_surveydealing kk ON a.ID = kk.APPLYID\
                  LEFT JOIN tio_xd_lb_msqkinfo l ON a.ID = l.APPLYID\
                  LEFT JOIN tio_xd_lb_applyinfoadditional taa ON a.ID = taa.APPLYID\
                  LEFT JOIN tio_xd_sys_org_his m ON (b.orgid = m.id AND m.starttime<a.CREATETIME AND a.CREATETIME<=m.endtime)\
                  LEFT JOIN tio_xd_sys_org_his n ON (m.pid = n.id AND n.starttime<a.CREATETIME AND a.CREATETIME<=n.endtime) \
                  left JOIN tio_xd_loanentry_order_info o ON a.`APPLYCODE` = o.`order_no`\
                  left join tio_xd_fdb_login_user p on o.entry_agent_id = p.id \
                  left join tpm_agent_info q on p.real_name_id = q.id \
                  left join tio_xd_order_belong_identify_info r on a.applycode = r.apply_code \
                  GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20190322 change  再贷的数据，直销坐席更新上笔订单的坐席
            sql = """update tb_applyinfo_rpt_temp a ,tb_applyinfo_rpt b
                      set a.DIRECTSELLER = b.DIRECTSELLER,a.DIRECT_EMPID = b.DIRECT_EMPID
                    where a.last_apply_id = b.applyid
                      and a.LOANTYPE = 4 and a.DIRECT_EMPID is null"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 车贷产品由于经纪人模式 9月到10月25日期间 经纪人直接加入的小贷员工信息表，以客户经理形式进件，25号后通过小程序进件，到小贷申请表时以邀请人进件
            # 25号前经纪人需特殊更新其邀请人相关信息
            # sql = """truncate table tbl_agent_emp_detail_temp"""
            # r = rpt_db.query(sql)
            # rpt_db.commit()

            clear_table('tbl_agent_emp_detail_temp')

            sql = """INSERT INTO tbl_agent_emp_detail_temp
                    (idcard,agentCode,agentName,inviterID,inviterCode,inviterName,inviterDuties,agentInvCode,newAgentID,newAgentCode,newAgentName,
                     invOrgID,invOrgLeaderID,invOrgLeader,invOrgName)
                    SELECT a.idcard, a.workCode agentCode,a.name agentName,d.id,a.upLevel inviterCode,b.name inviterName,IFNULL(d.DUTIES,b.positionName) inviterDuties,
                           c.workCode agentInvCode,e.id,/*CONCAT('MID',LPAD(e.id,8,0))*/g.broker_no,e.real_name,d.orgid,f.orgleaderid,f.orgleader,f.orgname
                      FROM tio_hr_sys_emp A
                      LEFT JOIN tio_hr_sys_emp b ON a.upLevel = b.workCode AND b.statDate = a.statDate
                      LEFT JOIN tio_hr_sys_emp c ON a.idcard = c.idcard AND c.statDate = a.statDate AND UPPER(c.workCode) LIKE 'WJ%' AND TIMESTAMPDIFF(DAY,c.leaveDate,a.hireDate)<=90 AND c.positionName='客户经理'
                      LEFT JOIN tio_xd_sys_emp d ON a.upLevel = IF (d.empno = 'WJ56813 ','WJ56813',d.empno)
                      LEFT JOIN tb_org_rpt f ON d.orgid = f.id
                      left JOIN tio_xd_fdb_real_name e ON a.idcard = e.id_code and e.approval_status = '06'
                      left join tio_xd_fdb_login_user g on g.real_name_id = e.id
                     WHERE A.statDate ='{statdate}'
                       AND UPPER(A.workCode) LIKE 'M%'
                     group by a.idcard""".format(statdate=statdate)
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 25号前经纪人进件数据，更新其经纪人，邀请人信息
            sql = """
              UPDATE tb_applyinfo_rpt_temp a, tbl_agent_emp_detail_temp b
                SET a.`inviterID` = b.`inviterID`,
                    a.`inviterCode` = b.`inviterCode`,
                    a.`inviterName` = b.`inviterName`,
                    a.`inviterDuties` = b.`inviterDuties`,
                    a.`agentID` = b.`newAgentID`,
                    a.`agentCode` = b.`newAgentCode`,
                    a.`agentName` = b.`newAgentName`,
                    a.orgid = b.invorgid,
                    a.orgleaderid = b.invOrgLeaderID,
                    a.orgleader = b.invOrgLeader,
                    a.orgname = b.invOrgName,
                    a.salesman = b.inviterID,
                    a.empno = b.`inviterCode`,
                    a.empname = b.inviterName,
                    a.inviter_empid = b.inviterID,
                    a.inviter_empcode = b.inviterCode,
                    a.inviter_empname = b.inviterName,
                    a.inviter_empduty = b.inviterDuties
                WHERE a.`EMPNO` = b.`agentCode`
                  AND a.`EMPNO` LIKE 'MID%'
                  AND b.newAgentID IS NOT NULL;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = """ UPDATE tb_applyinfo_rpt_temp a ,tbl_agent_emp_detail_temp b,tio_xd_sys_emp c ,tio_xd_sys_org d
                       SET a.`inviterID` = b.`inviterID`,a.`inviterCode`=b.`inviterCode`,a.`inviterName` = b.`inviterName`,a.`inviterDuties`=b.`inviterDuties`,
                           a.`agentID` = a.salesman,a.`agentCode` = a.`EMPNO`,a.`agentName` = a.`empname`,
                           a.orgid = c.orgid,a.orgleaderid = d.orgLeaderID,a.orgleader = d.OrgLeader,a.orgname = d.OrgName,
                           a.salesman = b.inviterID,a.empno = b.`inviterCode`,a.empname = b.inviterName,
                           a.inviter_empid = b.inviterID,a.inviter_empcode = b.inviterCode,a.inviter_empname = b.inviterName,a.inviter_empduty = b.inviterDuties
                      WHERE a.`EMPNO` = b.`agentCode`
                        and b.inviterID = c.id
                        and c.orgid = d.id
                        AND a.`EMPNO` LIKE 'MID%' and b.newAgentID is null"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 有一单进件 且经纪人工号不在HR系统的，通过身份证号在有一单查找其邀请人信息
            sql = """UPDATE tb_applyinfo_rpt_temp a ,tio_xd_sys_emp c ,tpm_agent_info d
                       SET a.`inviterID` = d.inviterID,a.`inviterCode`=d.`inviterCode`,a.`inviterName` = d.`inviterName`,a.`inviterDuties`=d.`inviterDuties`,
                           a.`agentID` = d.id,a.`agentCode` = d.agentcode,a.`agentName` = d.real_name,
                           a.orgid = d.orgid,a.orgleaderid = d.orgLeaderID,a.orgleader = d.OrgLeader,a.orgname = d.OrgName,
                           a.salesman = d.inviterID,a.empno = d.`inviterCode`,a.empname = d.inviterName,
                           a.inviter_empid=d.inviter_empid, a.inviter_empcode=d.inviter_empcode, a.inviter_empname=d.inviter_empname,a.inviter_empduty=d.inviter_empduty
                      WHERE a.`EMPNO` = c.EMPNO
                        and c.IDCARD = d.id_code
                        and d.approval_status = '06'
                        AND a.`EMPNO` LIKE 'MID%' and a.agentCode is null"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 经纪人是否90天内离职客户经理
            # HR系统录入的经纪人
            sql = """UPDATE tb_applyinfo_rpt_temp a ,tbl_agent_emp_detail_temp b
                      SET a.`empLeaveCode` = b.agentcode
                    WHERE a.empno = b.agentInvCode
                      and upper(a.empno) like 'WJ%'
                      AND a.`agentID` IS NULL"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 有一单邀请的经纪人
            sql = """update tb_applyinfo_rpt_temp a,tpm_agent_info b
                       set a.empLeaveCode = b.agentCode
                     where a.empno = b.empLeaveCode
                       and upper(a.empno) like 'WJ%'
                       and a.agentID is null """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 邀请人相关信息以邀请经纪人当时的组织、职位为准
            # sql = """update tb_applyinfo_rpt_temp a ,tpm_agent_info b
            #               set a.inviterDuties = b.inviterDuties,a.orgid = b.orgid ,a.orgname = b.orgname,
            #                   a.orgleaderid = b.orgleaderid,a.orgleader = b.orgleader
            #             where a.agentcode = b.agentcode
            #               and a.agentid is not null
            #        """
            # r = rpt_db.query(sql)
            # rpt_db.commit()

            # 新增车主信用贷数据
            # 20180322 change  贷款类型  默认是 新增贷款
            # 众邦车主信用贷  贷款类型与对应的车贷一致

            sql = "INSERT INTO tb_applyinfo_rpt_temp \
                                      (APPLYID,SITENO,SITENAME,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTID,CUSTNAME,DOCNO,ORGID,ORGNAME,ORGLEADERID,\
                                       ORGLEADER,LOANTYPE,SALESMAN,EMPNAME,AREACODENO,AREACODENAME,PRODUCTTYPE,\
                                       HKFS,DKFS,EMPNO,CARNO,CARBRAND,ASSPRICE,STATUS, SITELEADERID,\
                                       SITELEADER,PGJ,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,DIRECTSELLER,CREATETIME,\
                                       LOANTIME,DIRECT_EMPID,applycode,single_Premiums,\
                                       inviterDuties,inviterID,inviterCode,inviterName,agentID,agentCode,agentName,empLeaveCode,\
                                       inviter_empid, inviter_empcode, inviter_empname, inviter_empduty)\
                                SELECT a.ID,d.SITENO,d.SITENAME,concat('车主信用贷',a.repay_mode_name),'car_owner_credit','车主信用贷',\
                                       d.CUSTID,d.CUSTNAME,d.DOCNO, d.ORGID,d.ORGNAME,d.ORGLEADERID,d.ORGLEADER,\
                                       case when a.create_time>='2019-03-01' then d.LOANTYPE else '1' end LOANTYPE,\
                                       d.SALESMAN,d.EMPNAME,d.AREACODENO,d.AREACODENAME,'car_owner_credit' PRODUCTTYPE,\
                                       case when a.repay_mode_name = '先息后本' then '1'\
                                            when a.repay_mode_name = '等本等息' then '2' \
                                            when a.repay_mode_name = '到期一次性' then '3' \
                                            when a.repay_mode_name = '等额本息' then '4' else '0' end hkfs,\
                                       '信用',d.EMPNO,d.CARNO,d.CARBRAND,d.ASSPRICE,a.STATUS,d.SITELEADERID,\
                                       d.SITELEADER,d.PGJ,d.SOURCES,a.create_time AUDITTIME,a.AMOUNT,DATE_FORMAT(a.create_time,'%Y-%m-%d') APPLYDATE,\
                                       d.DIRECTSELLER,a.create_time CREATETIME,\
                                       e.`value` LOANTIME,d.DIRECT_EMPID,a.order_code applycode,0 single_Premiums,\
                                       d.inviterDuties,d.inviterID,d.inviterCode,d.inviterName,d.agentID,d.agentCode,d.agentName,d.empLeaveCode, \
                                       d.inviter_empid, d.inviter_empcode, d.inviter_empname, d.inviter_empduty\
                                  FROM tio_credit_order a\
                                  LEFT JOIN tio_credit_whitelist_detail b ON a.id = b.credit_order_id\
                                  left join tbl_carcredit_bug_data_detail f on a.id = f.orderid\
                                  LEFT JOIN tio_xd_lm_contract c ON ifnull(b.lms_contract_no,f.lmsContractNo) = c.contractno\
                                  LEFT JOIN tb_applyinfo_rpt d ON c.applyid = d.applyid \
                                  LEFT JOIN tio_credit_loan_term e ON a.loan_term_id = e.id \
                                 where a.product_code = 'car_owner_credit' and IFNULL(b.whitelist_type,'') <> 'XD_CDJQBMD' GROUP BY a.ID"
            print(sql)
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = "INSERT INTO tb_applyinfo_rpt_temp \
                                      (APPLYID,SITENO,SITENAME,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTID,CUSTNAME,DOCNO,LOANTYPE,PRODUCTTYPE,\
                                       HKFS,DKFS,STATUS, AUDITTIME,AMOUNT,APPLYDATE,CREATETIME,\
                                       LOANTIME,applycode,single_Premiums)\
                                SELECT a.ID,d.site_no,d.site_name,concat('车主信用贷',a.repay_mode_name),'car_owner_credit','车主信用贷',\
                                       d.cust_id,d.cust_name,d.cust_no,case when a.create_time>='2019-03-01' then '3' else '1' end LOANTYPE,\
                                       'car_owner_credit' PRODUCTTYPE,\
                                       case when a.repay_mode_name = '先息后本' then '1'\
                                            when a.repay_mode_name = '等本等息' then '2' \
                                            when a.repay_mode_name = '到期一次性' then '3' \
                                            when a.repay_mode_name = '等额本息' then '4' else '0' end hkfs,\
                                       '信用',a.STATUS,a.create_time AUDITTIME,a.AMOUNT,DATE_FORMAT(a.create_time,'%Y-%m-%d') APPLYDATE,\
                                       a.create_time CREATETIME,e.`value` LOANTIME,a.order_code applycode,0 single_Premiums\
                                  FROM tio_credit_order a\
                                  LEFT JOIN tio_credit_whitelist_detail b ON a.id = b.credit_order_id\
                                  LEFT JOIN tio_tn_fms_loan d ON a.id = d.order_id \
                                  LEFT JOIN tio_credit_loan_term e ON a.loan_term_id = e.id \
                                 where a.product_code = 'car_owner_credit' and b.whitelist_type = 'XD_CDJQBMD' GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 新增社保贷数据
            # 20180508 change  贷款类型  默认是 新增贷款
            sql = "INSERT INTO tb_applyinfo_rpt_temp \
                      (APPLYID,SITENO,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTNAME,DOCNO,LOANTYPE,PRODUCTTYPE,HKFS,DKFS,ASSPRICE,STATUS,\
                       PGJ,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,CREATETIME,LOANTIME,applycode,single_Premiums)\
                SELECT a.ID,'-',concat('很好借',a.repay_mode_name),'fast_loan','很好借',a.CUST_NAME,a.id_card,'1' LOANTYPE,'fast_loan' PRODUCTTYPE,\
                       case when a.repay_mode_name = '先息后本' then '1'\
                            when a.repay_mode_name = '等本等息' then '2' \
                            when a.repay_mode_name = '到期一次性' then '3' \
                            when a.repay_mode_name = '等额本息' then '4' \
                       else '0' end hkfs,\
                       '信用','0' ASSPRICE,a.STATUS,\
                       '0' PGJ,a.SOURCE,a.create_time AUDITTIME,a.AMOUNT,DATE_FORMAT(a.create_time,'%Y-%m-%d') APPLYDATE,\
                       a.create_time CREATETIME,e.`value` LOANTIME,a.order_code applycode,0 single_Premiums\
                  FROM tio_credit_order a\
                  LEFT JOIN tio_credit_loan_term e ON a.loan_term_id = e.id where a.product_code = 'fast_loan' GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180531 add 新增超越数
            sql = """
                insert into tb_applyinfo_rpt_temp
                      (APPLYID,SITENO,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTNAME,DOCNO,LOANTYPE,PRODUCTTYPE,HKFS,DKFS,ASSPRICE,STATUS,
                       PGJ,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,CREATETIME,LOANTIME,applycode,single_Premiums)
                select f.apply_id,'-',
                       concat(f.product_name,case f.repay_style when 3 then '先息后本' when 10 then '等本等息' when 6 then '到期一次性' when 1 then '等额本息' end) PRODUCTNAME,
                       f.product_type CONTRACTTEMPTYPE,
                       f.product_name DATAVALUE,f.cust_name,f.cust_no,f.loan_attr LOANTYPE,f.product_type,
                       case f.repay_style
                         when 3 then 1 -- 先息后本
                         when 10 then 2 -- 等本等息
                         when 6 then 3 -- 到期一次性
                         when 1 then 4 -- 等额本息
                         else 0
                       end hkfs,
                       '信用' DKFS,0 ASSPRICE,4 STATUS,
                       0 PGJ,null as SOURCE,f.addtime,f.loan_amt,date(f.addtime) APPLYDATE,
                       f.addtime CREATETIME,f.terms LOANTIME,f.apply_no,0 single_Premiums
                from tio_tn_crm_order_record r, tio_tn_fms_loan f
                where r.contract_no = f.contract_no
                group by f.apply_id;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180827 change   财务系统那边按财务要求更新了部分合同的产品名称，但是由于其他影响产品编号没调整，故BI这边数据处理时需把超月供产品编号调整
            sql = "update tb_applyinfo_rpt_temp a set a.CONTRACTTEMPTYPE = 'cys_fang',a.PRODUCTTYPE= 'cys_fang' where a.DATAVALUE = '超月供'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select APPLYID from tb_applyinfo_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['applyid'])

            if len(result_r) <= 1000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]贷款申请表tb_applyinfo_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_applyinfo_rpt')

                # sql = "delete a from tb_applyinfo_rpt a where a.applyid in(select b.applyid from tb_applyinfo_rpt_temp b)"
                # r = rpt_db.query(sql)
                # rpt_db.commit()

                sql_db = "insert into tb_applyinfo_rpt  \
                             (APPLYID,productarea,SITENO,SITENAME,PRODUCTID,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTID,CUSTNAME,DOCNO,\
                               ORGID,ORGNAME,ORGLEADERID,ORGLEADER,LASTAUDITRESULT,LASTAUDITTIME,LOANTYPE,\
                               SALESMAN,EMPNAME,AREACODENO,AREACODENAME,PRODUCTTYPE,ISSITE,ISAUDIT,FULLMEMBERTIME,HIGHEROFFICETIME,\
                               MOVETIME,HKFS,DKFS,EMPNO,CARNO,CARBRAND,FCDZ,FCZH,ASSPRICE,STATUS,\
                               SURVEYMAN,SURVEYTIME,SURVEYSTATUS,SURVEYUSER,MSMANID,MSMANNAME,MSTIME,CREATEBY,HKLX,SITELEADERID,\
                               SITELEADER,PGJ,DEPOSIT,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,DIRECTSELLER,CREATETIME,PHONECHECKUSER,\
                               LOANTIME,DIRECT_EMPID,applycode,single_Premiums,inviterID,inviterCode,inviterName,inviterDuties,\
                               agentID,agentName,agentCode,empLeaveCode,identify,PURPOSE,orgno,last_apply_id,reloan_flag,reloan_type,\
                               manage_fee_flag,loan_sources,inviter_empid, inviter_empcode, inviter_empname, inviter_empduty) \
                      select APPLYID,productarea,SITENO,SITENAME,PRODUCTID,PRODUCTNAME,CONTRACTTEMPTYPE,DATAVALUE,CUSTID,CUSTNAME,DOCNO,\
                               ORGID,ORGNAME,ORGLEADERID,ORGLEADER,LASTAUDITRESULT,LASTAUDITTIME,LOANTYPE,\
                               SALESMAN,EMPNAME,AREACODENO,AREACODENAME,PRODUCTTYPE,ISSITE,ISAUDIT,FULLMEMBERTIME,HIGHEROFFICETIME,\
                               MOVETIME,HKFS,DKFS,EMPNO,CARNO,CARBRAND,FCDZ,FCZH,ASSPRICE,STATUS,\
                               SURVEYMAN,SURVEYTIME,SURVEYSTATUS,SURVEYUSER,MSMANID,MSMANNAME,MSTIME,CREATEBY,HKLX,SITELEADERID,\
                               SITELEADER,PGJ,DEPOSIT,SOURCES,AUDITTIME,AMOUNT,APPLYDATE,DIRECTSELLER,CREATETIME,PHONECHECKUSER,\
                               LOANTIME,DIRECT_EMPID,applycode,single_Premiums,inviterID,inviterCode,inviterName,inviterDuties,\
                               agentID,agentName,agentCode,empLeaveCode,identify,PURPOSE,orgno,last_apply_id,reloan_flag,reloan_type,\
                               manage_fee_flag,loan_sources,inviter_empid, inviter_empcode, inviter_empname, inviter_empduty \
                        from tb_applyinfo_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]贷款申请表tb_applyinfo_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 合同表转化
def trans_contract():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]合同表tb_contract_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '合同表tb_contract_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '合同表tb_contract_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]合同表tb_contract_rpt->trytimes:', trytimes
        try:
            clear_table('tb_contract_rpt_temp')
            sql = "INSERT INTO tb_contract_rpt_temp \
                       (APPLYID, LOANSTATUS, RETURNSTATUS, STATUS, CHKSIGNDATE,CONTRACTNO, FEERATE, MANAGEFEERATE, AUDITAMT, RATE,\
                        CHKSIGNMAN, HKFS, AUDITLOANTIME, DKFS, AMT, TOTALFEE,SFAMT, ZQR, CJR, CONTRACTDATE,LOAN_AMT,FIRSTPAYED_STATUS,RERATE,CHECKDATE)\
                SELECT  APPLYID,LOANSTATUS,RETURNSTATUS,STATUS,\
                        (case when a.contracttemptype='antContractTemplate' and a.status=6 and a.CHKSIGNDATE is null then  DATE_FORMAT(createtime, '%Y-%m-%d') else DATE_FORMAT(CHKSIGNDATE, '%Y-%m-%d') end),\
                        CONTRACTNO,ifnull(FEERATE,0),ifnull(MANAGEFEERATE,0),AUDITAMT,ifnull(RATE,0),\
                        CHKSIGNMAN,HKFS,ifnull(AUDITLOANTIME,0)AUDITLOANTIME,DKFS,AMT,(select sum(s.SFAMT) from  tio_xd_lm_surcharge s where  s.APPLY_ID=a.APPLYID ),\
                        (SELECT SFAMT FROM ( SELECT other.SFAMT,other.APPLYID FROM tio_xd_lm_otherfee other WHERE other.SFITEM='服务费') b WHERE b.APPLYID=a.APPLYID) AS SFAMT,\
                        ZQR,CJR,DATE_FORMAT(CONTRACTDATE, '%Y-%m-%d'),LOAN_AMT,FIRSTPAYED_STATUS,RERATE,CHECKDATE \
                   from tio_xd_lm_contract a"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '小贷数据插入结束', datetime.datetime.today()

            # 插入车主信用贷订单系统中合同数据
            sql = "INSERT INTO tb_contract_rpt_temp \
                       (APPLYID, STATUS, CHKSIGNDATE,CONTRACTNO, FEERATE, MANAGEFEERATE, AUDITAMT, RATE,RERATE,\
                        HKFS, AUDITLOANTIME,DKFS,AMT, TOTALFEE,SFAMT, CONTRACTDATE,LOAN_AMT)\
                SELECT  a.id,if(b.STATUS = 2,6,b.status)status,\
                        DATE_FORMAT(b.create_time, '%Y-%m-%d'),\
                        b.contract_code CONTRACTNO,0 FEERATE,0 MANAGEFEERATE,0 AUDITAMT,a.total_rate RATE,a.total_rate RERATE,\
                        case when a.repay_mode_name = '先息后本' then '1'\
                             when a.repay_mode_name = '等本等息' then '2' \
                             when a.repay_mode_name = '到期一次性' then '3' \
                             when a.repay_mode_name = '等额本息' then '4' else '0' end hkfs,\
                        ifnull(c.`value`,0) AUDITLOANTIME,'信用',0 AMT,0 TOTALFEE,0 SFAMT,\
                        DATE_FORMAT(b.create_time, '%Y-%m-%d'),0 LOAN_AMT \
                   from tio_credit_order a \
                   left join tio_credit_contract b on a.id = b.order_id \
                   LEFT JOIN tio_credit_loan_term c ON a.loan_term_id = c.id where a.product_code in('car_owner_credit','fast_loan')"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '车主信用贷订单系统中合同数据插入结束', datetime.datetime.today()

            # 20180531 add 新增超越数,合同状态默认为6已签订，陈泽洪已确认
            sql = """
                insert into tb_contract_rpt_temp
                       (APPLYID, STATUS, CHKSIGNDATE,CONTRACTNO, FEERATE, MANAGEFEERATE, AUDITAMT, RATE,
                        HKFS, AUDITLOANTIME,DKFS,AMT, TOTALFEE,SFAMT, CONTRACTDATE,LOAN_AMT,RETURNSTATUS,LOANSTATUS)
                select f.apply_id,6 as STATUS,r.signing_date CHKSIGNDATE,r.contract_no,0 FEERATE,
                       r.service_fee*100 MANAGEFEERATE,r.loan_money AUDITAMT,round(r.year_rate/12*100,4) RATE,
                       case f.repay_style
                         when 3 then 1 -- 先息后本
                         when 10 then 2 -- 等本等息
                         when 6 then 3 -- 到期一次性
                         when 1 then 4 -- 等额本息
                         else 0
                       end hkfs,r.loan_term AUDITLOANTIME,'信用' DKFS,f.loan_amt AMT,0 TOTALFEE,0 SFAMT,date(r.signing_date) CONTRACTDATE,f.loan_amt LOAN_AMT,
                       CASE WHEN f.repay_status = 3 THEN 1 ELSE 0 END RETURNSTATUS,if(f.payment_status=3,1,0) LOANSTATUS
                from tio_tn_crm_order_record r, tio_tn_fms_loan f
                where r.contract_no = f.contract_no;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '超越数订单数据插入结束', datetime.datetime.today()

            # 更新水畔数据放款状态和回款状态,批核期数
            sql = "UPDATE tb_contract_rpt_temp a ,tio_tn_sp_loan b\
                   SET a.LOANSTATUS = b.payment_status,\
                       a.RETURNSTATUS = CASE WHEN b.repayment_status = 2 THEN 1 ELSE 0 END\
                 WHERE a.APPLYID = b.apply_id"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '更新水畔数据放款状态和回款状态结束', datetime.datetime.today()

            sql = "UPDATE tb_contract_rpt_temp a ,\
                       (select apply_id,max(terms)terms from tio_tn_sp_repay_plan group by apply_id) b,\
                       tio_tn_sp_loan c\
                   SET a.AUDITLOANTIME = b.terms\
                 WHERE a.APPLYID = b.apply_id \
                   and a.AUDITLOANTIME =0\
                   AND a.APPLYID = c.apply_id \
                   and c.product_type = 'P019'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '更新水畔数据批核期数结束', datetime.datetime.today()

            # 车主信用贷数据更新合同金额 、放款状态、结清状态
            sql = "UPDATE tb_contract_rpt_temp a ,tio_tn_fms_loan b\
                   SET a.LOANSTATUS = if(b.payment_status=3,1,0),\
                       a.amt = case when b.product_type ='car_owner_credit' and b.org_dept in ('zbyh_2','hbjz_zr','ry_zr','tounawang', 'scrb','ls_zr') then ifnull(b.loan_act_amt,0) else ifnull(b.loan_amt,0) end,\
                       a.AUDITAMT = case when b.product_type ='car_owner_credit' and b.org_dept in ('zbyh_2','hbjz_zr','ry_zr','tounawang', 'scrb','ls_zr') then ifnull(b.loan_act_amt,0) else ifnull(b.loan_amt,0) end,\
                       a.LOAN_AMT = ifnull(b.LOAN_AMT,0),\
                       a.RETURNSTATUS = CASE WHEN b.repay_status = 3 THEN 1 ELSE 0 END\
                 WHERE a.APPLYID = b.order_id\
                   AND EXISTS(SELECT 1 FROM tio_tn_fms_loan c WHERE a.APPLYID = c.order_id and c.product_type in('car_owner_credit','fast_loan'))"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '更新车主信用贷数据更新合同金额 、放款状态、结清状态结束', datetime.datetime.today()

            # 20181023 add 360车贷 360信息服务费   ，RE费率小贷合同表360贷存的是综合费率，该产品无RE费率
            sql = """UPDATE tb_contract_rpt_temp A ,tio_xd_lb_applyinfo b,tio_xd_sys_product c
                     SET A.serviceRate = GREATEST(round(IFNULL(a.RERATE,0) - IFNULL(A.RATE,0) - IFNULL(A.MANAGEFEERATE,0),6),0)
                   WHERE a.applyid = b.ID
                     and b.PRODUCTS = c.ID
                     and c.PRODUCTTYPE = 'P020'"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = """UPDATE tb_contract_rpt_temp A ,tio_xd_lb_applyinfo b,tio_xd_sys_product c
                     SET A.RERATE = null
                   WHERE a.applyid = b.ID
                     and b.PRODUCTS = c.ID
                     and c.PRODUCTTYPE = 'P020'"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '更新360车贷RE费用结束', datetime.datetime.today()

            # 判断是否有数据
            sql = "select ID from tb_contract_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]合同表tb_contract_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_contract_rpt')
                sql_db = "insert into tb_contract_rpt  \
                             (ID, APPLYID, LOANSTATUS, RETURNSTATUS, STATUS, CHKSIGNDATE,CONTRACTNO, FEERATE, MANAGEFEERATE, AUDITAMT, RATE,\
                              CHKSIGNMAN, HKFS, AUDITLOANTIME, DKFS, AMT, TOTALFEE,SFAMT, ZQR, CJR, CONTRACTDATE,LOAN_AMT,FIRSTPAYED_STATUS,\
                              RERATE,serviceRate,CHECKDATE) \
                      select ID, APPLYID, LOANSTATUS, RETURNSTATUS, STATUS, CHKSIGNDATE,CONTRACTNO, FEERATE, MANAGEFEERATE, AUDITAMT, RATE,\
                              CHKSIGNMAN, HKFS, AUDITLOANTIME, DKFS, AMT, TOTALFEE,SFAMT, ZQR, CJR, CONTRACTDATE,LOAN_AMT,FIRSTPAYED_STATUS,\
                              RERATE,serviceRate,CHECKDATE \
                        from tb_contract_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

                sql = "delete from tb_contract_rpt where CONTRACTNO = '';"
                rpt_db.updateBysql(sql)

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]合同表tb_contract_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 放款表转化
def trans_payment():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]放款表tb_payment_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '放款表tb_payment_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '放款表tb_payment_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]放款表tb_payment_rpt->trytimes:', trytimes
        try:
            clear_table('tb_payment_rpt_temp')
            sql = "INSERT INTO tb_payment_rpt_temp \
                       (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,PAYAMT1,ACTUALPAYAMT,CONFIRMDATE,STATUS,PAYTYPE,EXTSTATUS,EXHPAYTYPE,\
                       FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,YSXF,YSXFAMT,LOANDATE,PAYMENTCONFIRMTIME,LOANNO,REMARK,\
                       LEFTAMT,QYBZJ,RATE,PAYMODE,FKAPPLYDATE,AUDITSTATUS,AUDITBY,AUDITDATE,CONFIRMMAN,\
                       CONFIRMUSER,COMPANYBANKACCOUNT,COMPANYBANKNAME,exhbegin,risk_doc_upload_date)\
                SELECT a.ID,a.PAYMENTID,b.APPLYID,b.AMT,a.PAYAMT,a.PAYAMT,a.ACTUALPAYAMT,a.CONFIRMDATE,a.STATUS,a.PAYTYPE,c.STATUS,c.EXHPAYTYPE,\
                       b.FEERATE,b.FEES,b.MANAGEFEERATE,b.MANAGEFEES,b.YSXF,b.YSXFAMT,a.CONFIRMDATE,a.CONFIRMDATE,b.LOANNO,'小贷' REMARK,\
                       b.LEFTAMT,IF(b.QYBZJ IS NULL,0,b.QYBZJ),b.RATE,a.PAYMENT,a.FKAPPLYDATE,a.AUDITSTATUS,a.AUDITBY,a.AUDITDATE,a.CONFIRMMAN,\
                       a.CONFIRMUSER,d.BANKACCOUNT,d.BANKNAME,MAX(c.EXHBEGIN) AS EXHBEGIN,a.risk_doc_upload_date\
                  FROM tio_xd_lm_paymentdetail a \
                  LEFT JOIN tio_xd_lm_payment b ON a.PAYMENTID=b.ID \
                  LEFT JOIN tio_xd_sys_bankaccount d ON a.PAYBANKACCOUNT=d.ID \
                  LEFT JOIN tio_xd_lf_jextapplyinfo c ON b.ID=c.LOANID \
                 GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 新增水畔放款数据
            sql = "INSERT INTO tb_payment_rpt_temp\
                        (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,CONFIRMDATE,STATUS,PAYTYPE,\
                         FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,LOANNO,\
                         LEFTAMT,RATE,remark,QYBZJ,withdrawDate)	\
                SELECT a.`id`,a.id,a.`apply_id`,IFNULL(b.loan_amt,a.loan_amt),a.loan_amt,a.loan_act_amt,a.payment_time,a.payment_status,1,\
                       b.FEERATE,b.FEES,b.MANAGEFEERATE,b.MANAGEFEES,DATE_FORMAT(a.payment_time,'%Y-%m-%d'),a.payment_time,\
                       a.loan_apply_no,0 LEFTAMT,b.rate,'水畔',0,DATE_FORMAT(a.payment_time,'%Y-%m-%d')\
                  FROM tio_tn_sp_loan a \
                  LEFT JOIN tio_xd_lm_contract b ON a.`apply_id` = b.`APPLYID`\
                 WHERE a.`payment_status` = 1"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 新增车主信用贷数据
            # 首次放款 -- 前置费用   #再次放款 -- 余款
            # 20180420 change 之前的放款时间改取满标时间  新增提现时间 取log表中success_time   同一天多次提现的合并成一条
            sql = "INSERT INTO tb_payment_rpt_temp\
                        (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,withdrawDate,STATUS,PAYTYPE,\
                         FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,\
                         LEFTAMT,RATE,remark,QYBZJ,CONFIRMDATE)	\
                  select min(paymentDetailID),min(PAYMENTID),APPLYID,max(AMT),sum(PAYAMT),sum(ACTUALPAYAMT),min(withdrawDate),min(STATUS),\
                         PAYTYPE,min(FEERATE),min(FEES),min(MANAGEFEERATE),min(MANAGEFEES),min(LOANDATE),min(PAYMENTCONFIRMTIME),\
                         IFNULL(max(loan_act_amt),0)-IFNULL(SUM(LEFTAMT),0) LEFTAMT,min(RATE),min(remark),min(QYBZJ),CONFIRMDATE\
                   from (SELECT c.id paymentDetailID,a.id PAYMENTID,a.order_id APPLYID,IFNULL(a.loan_amt,0)AMT ,\
                                IFNULL(c.money,0)PAYAMT,IFNULL(c.money,0)ACTUALPAYAMT,\
                                case when a.payment_time>='2018-04-01' then DATE_FORMAT(a.borrow_full_time,'%Y-%m-%d') else DATE_FORMAT(c.successtime,'%Y-%m-%d') end withdrawDate,\
                                if(a.payment_status=3,1,0) STATUS,(case when c.catalog_type = 'FRONT_FEE' then 1 else 2 end) PAYTYPE,\
                                0 FEERATE,0 FEES,0 MANAGEFEERATE,0 MANAGEFEES,DATE_FORMAT(c.successtime,'%Y-%m-%d')  LOANDATE,\
                                c.successtime PAYMENTCONFIRMTIME,IFNULL(max(a.loan_act_amt),0) loan_act_amt,IFNULL(SUM(d.money),0) LEFTAMT,\
                                b.total_rate rate,'车主信用贷' remark,0 QYBZJ,DATE_FORMAT(c.successtime,'%Y-%m-%d') CONFIRMDATE\
                           FROM tio_tn_fms_loan a \
                           left join tio_credit_order b on a.order_id = b.id \
                           LEFT JOIN tio_tn_fms_loan_log c ON a.order_id = c.order_id AND c.status = 3 AND c.catalog_type IN('FRONT_FEE','AMOUNT')\
                           LEFT JOIN tio_tn_fms_loan_log d ON c.order_id = d.order_id AND c.id>=d.id AND d.status = 3 AND d.catalog_type IN('FRONT_FEE','AMOUNT')\
                          where a.product_type = 'car_owner_credit' and a.payment_status = 3  and ifnull(a.org_dept,'未知') NOT IN ('zbyh_2','hbjz_zr','ry_zr','tounawang','scrb','ls_zr')\
                          group by  c.id,a.id,a.order_id order by a.order_id,c.catalog_type)aa\
                  group by aa.applyid,aa.PAYTYPE,aa.CONFIRMDATE"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20190401  add 新增众邦车主信用贷数据
            # 首次放款 --  余款     前置费用 归属到预收管理费里面
            # 必须 tio_tn_fms_loan_log表中有catalog_type ='AMOUNT' 的类别提现成功的才展现
            sql = """INSERT INTO tb_payment_rpt_temp
                        (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,withdrawDate,STATUS,PAYTYPE,
                         FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,
                         LEFTAMT,RATE,remark,QYBZJ,CONFIRMDATE)
                  select min(paymentDetailID),min(PAYMENTID),APPLYID,max(AMT),sum(PAYAMT),sum(ACTUALPAYAMT),min(withdrawDate),min(STATUS),
                         PAYTYPE,min(FEERATE),min(FEES),min(MANAGEFEERATE),min(MANAGEFEES),min(LOANDATE),min(PAYMENTCONFIRMTIME),
                         IFNULL(max(loan_act_amt),0)-IFNULL(SUM(LEFTAMT),0) LEFTAMT,min(RATE),min(remark),min(QYBZJ),CONFIRMDATE
                   from (SELECT c.id paymentDetailID,a.id PAYMENTID,a.order_id APPLYID,IFNULL(a.loan_act_amt,0)AMT ,
                                IFNULL(c.money,0)PAYAMT,IFNULL(c.money,0)ACTUALPAYAMT,
                                case when a.payment_time>='2018-04-01' then DATE_FORMAT(a.borrow_full_time,'%Y-%m-%d') else DATE_FORMAT(c.successtime,'%Y-%m-%d') end withdrawDate,
                                if(a.payment_status=3,1,0) STATUS,1 PAYTYPE,
                                0 FEERATE,0 FEES,0 MANAGEFEERATE,d.money MANAGEFEES,DATE_FORMAT(c.successtime,'%Y-%m-%d')  LOANDATE,
                                c.successtime PAYMENTCONFIRMTIME,IFNULL(max(a.loan_act_amt),0) loan_act_amt,IFNULL(SUM(c.money),0) LEFTAMT,
                                b.total_rate rate,'众邦车主信用贷' remark,0 QYBZJ,DATE_FORMAT(c.successtime,'%Y-%m-%d') CONFIRMDATE
                           FROM tio_tn_fms_loan a
                           left join tio_credit_order b on a.order_id = b.id
                        inner JOIN tio_tn_fms_loan_log c ON a.order_id = c.order_id AND c.status = 3 AND c.catalog_type ='AMOUNT'
                           LEFT JOIN tio_tn_fms_loan_log d ON c.order_id = d.order_id AND d.status = 3 AND d.catalog_type ='FRONT_FEE'
                          where a.product_type = 'car_owner_credit' and a.payment_status = 3 and a.org_dept IN ('zbyh_2','hbjz_zr','ry_zr','tounawang','scrb','ls_zr')
                          group by  c.id,a.id,a.order_id order by a.order_id,c.catalog_type)aa
                  group by aa.applyid,aa.PAYTYPE,aa.CONFIRMDATE"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 新增社保贷数据
            # 首次放款  1放
            # 20180508 add
            sql = "INSERT INTO tb_payment_rpt_temp\
                       (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,withdrawDate,STATUS,PAYTYPE,\
                        FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,\
                        LEFTAMT,RATE,remark,QYBZJ,CONFIRMDATE)	\
                select min(paymentDetailID),min(PAYMENTID),APPLYID,max(AMT),sum(PAYAMT),sum(ACTUALPAYAMT),min(withdrawDate),min(STATUS),\
                       PAYTYPE,min(FEERATE),min(FEES),min(MANAGEFEERATE),min(MANAGEFEES),min(LOANDATE),min(PAYMENTCONFIRMTIME),\
                       min(LEFTAMT),min(RATE),min(remark),min(QYBZJ),CONFIRMDATE \
                 from (SELECT c.id paymentDetailID,a.id PAYMENTID,a.order_id APPLYID,IFNULL(a.loan_amt,0)AMT ,\
                              IFNULL(c.money,0)PAYAMT,IFNULL(c.money,0)ACTUALPAYAMT,\
                              DATE_FORMAT(a.borrow_full_time,'%Y-%m-%d') withdrawDate,if(a.payment_status=3,1,0) STATUS,1 PAYTYPE,\
                              0 FEERATE,0 FEES,0 MANAGEFEERATE,0 MANAGEFEES,DATE_FORMAT(c.successtime,'%Y-%m-%d')  LOANDATE,\
                              c.successtime PAYMENTCONFIRMTIME,IFNULL(max(a.loan_amt),0)-IFNULL(SUM(c.money),0) LEFTAMT,\
                              b.total_rate rate,'很好借' remark,0 QYBZJ,DATE_FORMAT(c.successtime,'%Y-%m-%d') CONFIRMDATE\
                         FROM tio_tn_fms_loan a \
                         left join tio_credit_order b on a.order_id = b.id \
                         LEFT JOIN tio_tn_fms_loan_log c ON a.order_id = c.order_id AND c.status = 3 AND c.catalog_type = 'AMOUNT'\
                        where a.product_type = 'fast_loan' and a.payment_status = 3 group by  c.id,a.id,a.order_id)aa\
                group by aa.applyid,aa.PAYTYPE,aa.CONFIRMDATE"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180531 add 新增超越数
            sql = """
                insert into tb_payment_rpt_temp
                       (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,withdrawDate,STATUS,PAYTYPE,
                        FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,
                        LEFTAMT,RATE,remark,QYBZJ,CONFIRMDATE)
                select min(paymentDetailID),min(PAYMENTID),apply_id,max(AMT),sum(PAYAMT),
                       sum(ACTUALPAYAMT),min(withdrawDate),min(STATUS),PAYTYPE,0 FEERATE,0 FEES,0 MANAGEFEERATE,0 MANAGEFEES,
                       min(LOANDATE),min(PAYMENTCONFIRMTIME),min(LEFTAMT),min(RATE),min(remark),min(QYBZJ),CONFIRMDATE
                  from (select c.id paymentDetailID,b.id PAYMENTID,b.apply_id,ifnull(b.loan_amt,0) AMT,IFNULL(c.money,0) PAYAMT,
                               IFNULL(c.money,0) ACTUALPAYAMT,date(b.borrow_full_time) withdrawDate,if(b.payment_status=3,1,0) STATUS,1 PAYTYPE,
                               date(c.successtime) LOANDATE,c.successtime PAYMENTCONFIRMTIME,
                               max(b.loan_amt)-sum(d.money) LEFTAMT,round(b.loan_apr/12*100,4) RATE,'超越数' remark,0 QYBZJ,date(c.successtime) CONFIRMDATE
                          from tio_tn_fms_loan b
                          left join tio_tn_fms_loan_log c on b.order_id = c.order_id and c.status = 3 and c.catalog_type = 'AMOUNT'
                          left join tio_tn_fms_loan_log d on c.order_id = d.order_id and c.id>=d.id and d.status = 3 and d.catalog_type = 'AMOUNT'
                         where b.payment_status = 3
                           and b.product_type in ('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan')
                         group by c.id,b.id,b.apply_id)a
                 group by a.apply_id,a.PAYTYPE,a.CONFIRMDATE;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20181020 add 新增360车贷
            sql = """
                insert into tb_payment_rpt_temp
                       (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,ACTUALPAYAMT,withdrawDate,STATUS,PAYTYPE,
                        FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,LOANDATE,PAYMENTCONFIRMTIME,
                        LEFTAMT,RATE,remark,QYBZJ,CONFIRMDATE)
                select min(paymentDetailID),min(PAYMENTID),apply_id,max(AMT),sum(PAYAMT),
                       sum(ACTUALPAYAMT),min(withdrawDate),min(STATUS),PAYTYPE,FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,
                       min(LOANDATE),min(PAYMENTCONFIRMTIME),min(LEFTAMT),min(RATE),min(remark),min(QYBZJ),CONFIRMDATE
                  from (select min(c.id) paymentDetailID,b.id PAYMENTID,b.apply_id,ifnull(b.loan_act_amt,0) AMT,sum(IFNULL(c.money,0)) PAYAMT,
                               sum(IFNULL(c.money,0)) ACTUALPAYAMT,date(b.borrow_full_time) withdrawDate,if(b.payment_status=3,1,0) STATUS,1 PAYTYPE,
                               date(b.borrow_full_time) LOANDATE,b.borrow_full_time PAYMENTCONFIRMTIME,
                               max(b.loan_act_amt)-sum(c.money) LEFTAMT,e.RATE,e.FEERATE,e.MANAGEFEERATE,e.FEES,e.MANAGEFEES,'360贷' remark,0 QYBZJ,date(b.borrow_full_time) CONFIRMDATE
                          from tio_tn_fms_loan b
                          left join tio_tn_fms_loan_log c on b.order_id = c.order_id and c.status = 3 and c.catalog_type = 'AMOUNT'
                          left join tio_xd_lm_contract e on b.apply_id = e.applyid and e.status = 6
                         where b.payment_status = 3
                           and b.product_type  = '360_car_loan'
                         group by b.id,b.apply_id)a
                 group by a.apply_id,a.PAYTYPE,a.CONFIRMDATE
                """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20171226 车主信用贷数据 如果余款未放完，则该数据不获取
            sql = "DELETE AA FROM tb_payment_rpt_temp AA ,\
                       (SELECT * FROM tb_payment_rpt_temp A \
                         WHERE A.remark = '车主信用贷' \
                           AND A.paytype = 1\
                           AND NOT EXISTS(SELECT b.paymentid FROM tb_payment_rpt_temp b \
                                           WHERE A.paymentid =B.paymentid AND b.paytype = 2 AND b.remark = '车主信用贷'))BB\
                 WHERE AA.`APPLYID` = BB.APPLYID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180420 CHANGE 20180401及之后除水畔外数据  放款时间 改成取投哪系统中标的满标时间   提现时间  取投哪系统tn_lend_log中success_time
            sql = "UPDATE tb_payment_rpt_temp a,\
                       (SELECT applyid,MIN(CONFIRMDATE)CONFIRMDATE FROM tb_payment_rpt_temp WHERE paytype IN(1,2) AND CONFIRMDATE<='2018-03-31' GROUP BY applyid) b\
                   SET a.withdrawDate = b.CONFIRMDATE \
                 WHERE a.applyid = b.applyid\
                   AND a.CONFIRMDATE<='2018-03-31'"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 水畔数据 满标时间 = 放款时间
            sql = "update tb_payment_rpt_temp a set a.withdrawDate = a.CONFIRMDATE where A.remark in('水畔','360贷')"
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '1'
            # 车贷 数据更新 满标时间
            sql = "UPDATE tb_payment_rpt_temp a ,tio_tn_tn_lend b,tio_tn_dw_borrow c\
                   SET a.withdrawDate = DATE_FORMAT(c.`success_time`,'%Y-%m-%d')\
                 WHERE a.`PAYMENTID` = cast(b.paymentid as SIGNED)\
                   AND b.`borrowid` = c.`id` \
                   AND c.`status` IN(3,73)\
                   AND (b.exhi_type =0 or b.exhi_type is null) \
                   AND c.`success_time`>='2018-04-01'\
                   and substr(b.paymentid,1,1) in('1','2','3','4','5','6','7','8','9')\
                   AND a.`REMARK` = '小贷'"
            r = rpt_db.query(sql)
            rpt_db.commit()
            print '2'
            sql = """
              update tb_payment_rpt_temp set CONFIRMDATE=null where REMARK = '小贷' AND withdrawDate>='2018-04-01'
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20191016 很好借产品不存在待放尾款，部分客户出现金额是因为客户借款金额大于5W需要分多笔提现导致，需要重新调整代码。
            sql = "UPDATE tb_payment_rpt_temp a \
                               SET a.leftamt = 0 \
                             WHERE A.remark = '很好借' \
                               "
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 车贷 数据更新 提现时间
            # sql = "UPDATE tb_payment_rpt_temp a ,\
            #            (SELECT out_lend_id,lend_type,Min(successtime)success_time \
            #               FROM tio_tn_tn_lend_log\
            #              WHERE successtime>='2018-04-01'\
            #                AND TYPE = 3\
            #                AND lend_type IN(1,2)\
            #                AND STATUS = 3 \
            #              GROUP BY out_lend_id,lend_type) b\
            #        SET a.`CONFIRMDATE` = b.success_time\
            #      WHERE a.`paymentDetailID` = cast(b.out_lend_id as SIGNED)\
            #        and substr(b.out_lend_id,1,1) in('1','2','3','4','5','6','7','8','9')\
            #        AND a.`PAYTYPE` = b.lend_type\
            #        AND a.`REMARK` = '小贷'\
            #        AND a.withdrawDate>='2018-04-01'"
            # r = rpt_db.query(sql)
            # rpt_db.commit()

            clear_table('tbl_tn_lend_log_temp')
            sql = """INSERT INTO tbl_tn_lend_log_temp(out_lend_id,lend_type,successtime)
                        SELECT CAST(out_lend_id AS SIGNED) out_lend_id,lend_type,MIN(successtime)success_time
                          FROM tio_tn_tn_lend_log
                         WHERE successtime>='2018-04-01'
                           AND TYPE = 3
                           AND lend_type IN(1,2)
                           AND STATUS = 3
                           AND SUBSTR(out_lend_id,1,1) IN('1','2','3','4','5','6','7','8','9')
                         GROUP BY CAST(out_lend_id AS SIGNED),lend_type"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = """UPDATE tb_payment_rpt_temp a ,tbl_tn_lend_log_temp b
                       SET a.`CONFIRMDATE` = b.successtime
                     WHERE a.`paymentDetailID` = b.out_lend_id
                       AND a.`PAYTYPE` = b.lend_type
                       AND a.`REMARK` = '小贷'
                       AND a.withdrawDate>='2018-04-01'"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            print '3'

            # 20180503 满标时间是空的，找之前是否有过满标时间
            sql = "UPDATE tb_payment_rpt_temp a,\
                           (SELECT applyid,MIN(withdrawDate)withdrawDate FROM tb_payment_rpt_temp \
                             WHERE paytype IN(1,2) and withdrawDate is not null GROUP BY applyid) b\
                       SET a.withdrawDate = b.withdrawDate \
                     WHERE a.applyid = b.applyid\
                       AND a.withdrawDate is null"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180503 满标时间是空的，用放款时间更新
            sql = "UPDATE tb_payment_rpt_temp a,\
                       (SELECT applyid,MIN(CONFIRMDATE)CONFIRMDATE FROM tb_payment_rpt_temp \
                         WHERE paytype IN(1,2) GROUP BY applyid) b\
                   SET a.withdrawDate = b.CONFIRMDATE \
                 WHERE a.applyid = b.applyid\
                   AND a.withdrawDate is null"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180705 更改首次放款的放款金额，剔除管理费(绩效核算需求)
            sql = """
              update tb_payment_rpt_temp as a
                join (select a.APPLYID, max(a.AMT)-sum(if(b.PAYTYPE=1,0,b.PAYAMT))-max(a.LEFTAMT) payamt
                        from tio_xd_lm_payment a, tio_xd_lm_paymentdetail b
                       where b.PAYMENTID = a.ID
                         and b.STATUS in (0,1)
                      group by a.APPLYID)b
                  on a.APPLYID = b.applyid and a.PAYTYPE = 1 and a.REMARK = '小贷'
              set a.PAYAMT = b.payamt;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_payment_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]放款表tb_payment_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_payment_rpt')
                sql_db = "insert into tb_payment_rpt  \
                             (paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,PAYAMT1,ACTUALPAYAMT,CONFIRMDATE,STATUS,PAYTYPE,EXTSTATUS,EXHPAYTYPE,\
                               FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,YSXF,YSXFAMT,LOANDATE,PAYMENTCONFIRMTIME,LOANNO,REMARK,\
                               LEFTAMT,QYBZJ,RATE,PAYMODE,FKAPPLYDATE,AUDITSTATUS,AUDITBY,AUDITDATE,CONFIRMMAN,\
                               CONFIRMUSER,COMPANYBANKACCOUNT,COMPANYBANKNAME,exhbegin,risk_doc_upload_date,withdrawDate) \
                      select paymentDetailID,PAYMENTID,APPLYID,AMT,PAYAMT,PAYAMT1,ACTUALPAYAMT,CONFIRMDATE,STATUS,PAYTYPE,EXTSTATUS,EXHPAYTYPE,\
                               FEERATE,FEES,MANAGEFEERATE,MANAGEFEES,YSXF,YSXFAMT,LOANDATE,PAYMENTCONFIRMTIME,LOANNO,REMARK,\
                               LEFTAMT,QYBZJ,RATE,PAYMODE,FKAPPLYDATE,AUDITSTATUS,AUDITBY,AUDITDATE,CONFIRMMAN,\
                               CONFIRMUSER,COMPANYBANKACCOUNT,COMPANYBANKNAME,exhbegin,risk_doc_upload_date,withdrawDate \
                        from tb_payment_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]放款表tb_payment_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# GPS安装记录表tb_gpsinstall_rpt
def trans_gpsinstall():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]GPS安装记录表tb_gpsinstall_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'GPS安装记录表tb_gpsinstall_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'GPS安装记录表tb_gpsinstall_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]GPS安装记录表tb_gpsinstall_rpt->trytimes:', trytimes
        try:
            clear_table('tb_gpsinstall_rpt_temp')
            sql = "INSERT INTO tb_gpsinstall_rpt_temp \
                       (ID,APPLYID,CONTRACTNO,CARNO,CUSTNAME,SITENO,SUBMIT_TIME,INSTALL_NO,INSTALL_NAME,STATUS,ALTER_TIME)\
                SELECT ID,APPLYID,CONTRACTNO,CARNO,CUSTNAME,SITENO,SUBMIT_TIME,INSTALL_NO,INSTALL_NAME,STATUS,ALTER_TIME \
                  FROM tio_xd_lb_gps_install WHERE SUBMIT_TIME is not null and CONTRACTNO is not null AND STATUS=1"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_gpsinstall_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]GPS安装记录表tb_gpsinstall_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_gpsinstall_rpt')
                sql_db = "insert into tb_gpsinstall_rpt  \
                             (ID,APPLYID,CONTRACTNO,CARNO,CUSTNAME,SITENO,SUBMIT_TIME,INSTALL_NO,INSTALL_NAME,STATUS,ALTER_TIME) \
                      select ID,APPLYID,CONTRACTNO,CARNO,CUSTNAME,SITENO,SUBMIT_TIME,INSTALL_NO,INSTALL_NAME,STATUS,ALTER_TIME \
                        from tb_gpsinstall_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]GPS安装记录表tb_gpsinstall_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 回款表tb_return_rpt
def trans_return():
    reload(sys)
    sys.setdefaultencoding('utf8')
    today = datetime.date.today()
    hour = datetime.datetime.now().strftime("%H%M")
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]回款表tb_return_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '回款表tb_return_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '回款表tb_return_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]回款表tb_return_rpt->trytimes:', trytimes
        try:
            clear_table('tb_return_rpt_temp')
            sql = "INSERT INTO tb_return_rpt_temp \
                       (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,TOTALAMT,\
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,SHOULDPENATY, \
                        ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount)\
                SELECT  a.id,b.APPLYID,b.LOANID,b.PAYPHASES,b.TOTALPHASES,b.STATUS,DATE_FORMAT(a.paydate, '%Y-%m-%d'),a.ENDDATE,a.SHOULDDELAYINT,\
                        b.TOTALAMT,ifnull(a.ACTCAPITAL,0),a.AMT,DATE_FORMAT(a.SHOULDPAYDATE, '%Y-%m-%d'),ifnull(a.SHOULDCAPITAL,0),a.ISDELAY,\
                        ifnull(a.SHOULDINT,0),ifnull(a.ACTMANAGE,0),a.ACTDELAYINT,ifnull(a.ACTTCF,0),a.ACTDELAYAMT,a.ACTSMCSF,ifnull(a.ACTGPSF,0),a.ACTTCHF,a.ACTSCF,a.ACTKDF,ifnull(a.ACTINT,0),\
                        a.SHOULDGLF,ifnull(a.SHOULDTCF,0),a.SHOULDDELAYAMT,a.SHOULDSMCSF,ifnull(a.SHOULDGPSF,0),a.SHOULDTCHF,a.SHOULDSCF,a.SHOULDKDF,ifnull(a.SHOULDMANAGE,0),\
                        a.PAYMODE,a.BEOVERDUE,a.TOTALSHOULD,a.TOTALACT,b.TOTALREDDELAYINT,\
                        a.SHOULDKCF,a.ACTKCF,a.SHOULDDYF,a.ACTDYF,a.SHOULDDQX,a.ACTDQX,a.SHOULDXZF,a.ACTXZF,a.SHOULDPGF,a.ACTPGF,a.SHOULDSPF,a.ACTSPF,\
                        a.SHOULDLSF,a.ACTLSF,a.SHOULDJJF,a.ACTJJF,a.SHOULDGZF,a.ACTGZF,a.SHOULDFIRSTFEES,a.ACTFIRSTFEES,a.SHOULDQTF,a.ACTQTF,SHOULDKDF,ACTKDF,SHOULDPENATY,ACTPENATY,\
                        DATE_FORMAT(b.update_time, '%Y-%m-%d'),b.TOTALREDFORFEIT,a.ACTFXJ,a.SHOULDFXJ,a.SHOULDCARCOST,a.ACTCARCOST,c.BANKACCOUNT\
                   from tio_xd_lf_returndetail a \
                   join tio_xd_lf_return b on a.RETURNID=b.ID \
                   left join tio_xd_sys_bankaccount c on b.ACCOUNTID=c.id \
                  GROUP BY a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180323  change  水畔回款明细 与车贷不同，回款数据是个累加值  每次实际回款=截止本次还款金额-上一次还款金额
            sql = "INSERT INTO tb_return_rpt_temp\
                        (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,SHOULDDELAYINT,TOTALAMT,\
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,BEOVERDUE,\
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ, \
                        ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST)\
                  SELECT aaa.id,aaa.apply_id,aaa.loan_id,aaa.current_term,aaa.terms,aaa.STATUS,aaa.repay_act_time PAYDATE,\
                         IFNULL(aaa.ovd_total_fee,0) SHOULDDELAYINT,\
                         IFNULL(aaa.repay_act_amt,0) TOTALAMT,\
                         IFNULL(aaa.repay_act_capital,0)ACTCAPITAL,\
                         IFNULL(aaa.repay_act_amt,0)AMT,\
                         aaa.repay_due_time SHOULDPAYDATE,\
                         IFNULL(aaa.repay_total_capital,0) SHOULDCAPITAL,\
                         (CASE WHEN aaa.status= 0 THEN 1 ELSE 0 END) ISDELAY,\
                         IFNULL(aaa.repay_total_interest,0) SHOULDINT,\
                         0 ACTMANAGE,\
                         IFNULL(aaa.ovd_yes_fee,0)ACTDELAYINT,\
                         0 ACTTCF,0 ACTDELAYAMT,0 ACTSMCSF,0 ACTGPSF,0 ACTTCHF,0 ACTSCF,0 ACTKDF,\
                         IFNULL(aaa.repay_act_interest,0)ACTINT,\
                         0 SHOULDGLF,0 SHOULDTCF,0 SHOULDDELAYAMT,0 SHOULDSMCSF,0 SHOULDGPSF,0 SHOULDTCHF,0 SHOULDSCF,0 SHOULDKDF,0 SHOULDMANAGE,\
                         IFNULL(aaa.ovd_days,0) BEOVERDUE,\
                         IFNULL(aaa.repay_total_amt,0) TOTALSHOULD,\
                         IFNULL(aaa.repay_act_amt,0)TOTALACT,\
                         0 TOTALREDDELAYINT,0 SHOULDKCF,0 ACTKCF ,0 SHOULDDYF,0 ACTDYF,0 SHOULDDQX,0 ACTDQX,0 SHOULDXZF,\
                         0 ACTXZF, 0 SHOULDPGF,0 ACTPGF,0 SHOULDSPF,0 ACTSPF,0 SHOULDLSF,0 ACTLSF,0 SHOULDJJF,0 ACTJJF,\
                         0 SHOULDGZF,0 ACTGZF, 0 SHOULDFIRSTFEES,0 ACTFIRSTFEES,0 SHOULDQTF,0 ACTQTF,0 SHOULDFXJ,0 ACTFXJ,\
                         IFNULL(aaa.ovd_yes_fee,0)ACTPENATY,\
                         aaa.last_uptime update_time,\
                         0 TOTALREDFORFEIT1,0 ACTZQFXJ,0 SHOULDZQFXJ,0 SHOULDCARCOST,0 ACTCARCOST\
                 FROM(SELECT aa.* \
                        FROM (SELECT a.id,c.apply_id, a.loan_id,a.repay_id,c.terms,a.current_term,b.addtime last_uptime2,a.addtime last_uptime,a.repay_due_time,a.repay_act_time,\
                                     2 STATUS,a.ovd_days,\
                                     CASE WHEN b.repay_total_amt IS NULL THEN c.repay_total_amt ELSE c.repay_total_amt-b.repay_act_amt END repay_total_amt,\
                                     a.repay_act_amt-IFNULL(b.repay_act_amt,0) repay_act_amt,\
                                     CASE WHEN b.repay_total_capital IS NULL THEN c.repay_total_capital ELSE c.repay_total_capital-b.repay_act_capital END repay_total_capital,\
                                     a.repay_act_capital-IFNULL(b.repay_act_capital,0) repay_act_capital,\
                                     CASE WHEN b.repay_total_interest IS NULL THEN c.repay_total_interest ELSE c.repay_total_interest-b.repay_act_interest END repay_total_interest,\
                                     a.repay_act_interest-IFNULL(b.repay_act_interest,0) repay_act_interest,\
                                     CASE WHEN b.ovd_total_fee IS NULL THEN c.ovd_total_fee ELSE c.ovd_total_fee-b.ovd_total_fee END ovd_total_fee,\
                                     a.ovd_yes_fee-IFNULL(b.ovd_yes_fee,0) ovd_yes_fee,\
                                     IFNULL(b.prepay_fee,0)prepay_fee,a.prepay_act_fee \
                                FROM tio_tn_sp_repay_plan c\
                               INNER JOIN tio_tn_sp_repay_return a ON a.LOAN_ID = C.LOAN_ID AND A.current_term = C.current_term \
                                LEFT JOIN tio_tn_sp_repay_return b ON a.LOAN_ID = B.LOAN_ID AND A.current_term = B.current_term AND a.addtime>b.addtime)aa\
                       INNER JOIN (SELECT a.id, MAX(b.addtime)last_uptime \
                                     FROM tio_tn_sp_repay_plan c\
                                    INNER JOIN tio_tn_sp_repay_return a ON a.LOAN_ID = C.LOAN_ID AND A.current_term = C.current_term\
                                     LEFT JOIN tio_tn_sp_repay_return b ON a.LOAN_ID = B.LOAN_ID AND A.current_term = B.current_term AND a.addtime>b.addtime\
                                    GROUP BY a.id\
                                    )bb ON aa.id = bb.id AND IFNULL(aa.last_uptime2,NOW()) = IFNULL(bb.last_uptime,NOW())\
                       UNION ALL\
                      SELECT NULL,a.apply_id ,a.loan_id,a.id,a.terms,a.current_term,a.addtime last_uptime,a.addtime last_uptime,a.repay_due_time,CURDATE(),\
                             CASE WHEN a.repay_status = 3 THEN 0\
                                  WHEN a.repay_status = 1 AND a.repay_total_amt>a.repay_should_amt THEN -1\
                             ELSE -1 END STATUS,a.ovd_days,\
                             a.repay_should_amt,0,a.repay_should_capital,0,\
                             a.repay_should_interest,0,a.ovd_should_fee,0,0,0\
                        FROM tio_tn_sp_repay_plan a \
                       WHERE a.repay_status<>2\
                         AND a.repay_should_amt>0)aaa"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 插入车主信用贷数据
            # 由于车主信用贷回款流水表tio_tn_fms_repay_return中只有已还记录，如果有部分未还，体现不了，需要进行以下处理
            # tio_tn_fms_repay_return表中有回款记录的 ，status状态默认为2已还款
            # 如果某一期在tio_tn_fms_repay_return表中有回款记录，但是该期在tio_tn_fms_repay_plan表中状态是未还清的，则需要再将计划表该期应还记录插入回款流水中
            # 20180508 add 社保贷数据
            sql = "INSERT INTO tb_return_rpt_temp\
                       (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,SHOULDDELAYINT,TOTALAMT,\
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,shouldpenaty, \
                        ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST)\
                 SELECT b.id,e.order_id,a.loan_id,a.current_term,a.terms,\
                       CASE WHEN b.id is not null then 2\
                            when b.id is null and a.`repay_status` =10  THEN 2 \
                            WHEN b.id is null and a.`repay_status` = 0  THEN -1 ELSE 0 END STATUS, \
                       DATE_FORMAT(IFNULL(b.repay_act_time,a.repay_due_time),'%Y-%m-%d')PAYDATE, \
                       IFNULL(b.ovd_due_interest,a.ovd_due_interest)SHOULDDELAYINT, \
                       IFNULL(b.repay_act_amount,a.repay_act_amount) TOTALAMT, \
                       IFNULL(b.repay_act_capital,a.repay_act_capital) ACTCAPITAL, \
                       IFNULL(b.repay_due_amount,a.repay_due_amount)AMT, \
                       DATE_FORMAT(a.repay_due_time,'%Y-%m-%d')SHOULDPAYDATE, \
                       IFNULL(b.repay_due_capital,a.repay_due_capital) SHOULDCAPITAL, \
                       (CASE WHEN b.id IS NULL THEN a.is_delay WHEN b.id IS NOT NULL AND b.ovd_days>0 THEN 1 ELSE 0 END) ISDELAY, \
                       IFNULL(b.repay_due_interest,a.repay_due_interest) SHOULDINT, \
                       IFNULL(d.ACTMANAGE,0) ACTMANAGE, \
                       IFNULL(b.ovd_act_interest,a.ovd_act_interest)ACTDELAYINT, 0 ACTTCF, \
                       IFNULL(b.prepay_act_fee,a.prepay_act_fee)ACTDELAYAMT, \
                       IFNULL(d.ACTSMCSF,0) ACTSMCSF,\
                       0 ACTGPSF,\
                       IFNULL(d.ACTTCHF,0) ACTTCHF,\
                       0 ACTSCF,0 ACTKDF, \
                       IFNULL(b.repay_act_interest,a.repay_act_interest)ACTINT, \
                       0 SHOULDGLF,0 SHOULDTCF, \
                       IFNULL(b.prepay_due_fee,a.prepay_due_fee)SHOULDDELAYAMT, \
                       IFNULL(d.SHOULDSMCSF,c.SHOULDSMCSF) SHOULDSMCSF, \
                       0 SHOULDGPSF,\
                       IFNULL(d.SHOULDTCHF,c.SHOULDTCHF) SHOULDTCHF,\
                       0 SHOULDSCF,0 SHOULDKDF, \
                       ifnull(IFNULL(d.SHOULDMANAGE,c.SHOULDMANAGE),0) SHOULDMANAGE, \
                       '3' PAYMODE,IFNULL(b.ovd_days,a.ovd_days) BEOVERDUE, \
                       IFNULL(b.repay_due_amount,a.repay_due_amount)TOTALSHOULD, \
                       IFNULL(b.repay_act_amount,a.repay_act_amount)TOTALACT, \
                       0 TOTALREDDELAYINT,0 SHOULDKCF,0 ACTKCF ,0 SHOULDDYF,0 ACTDYF,0 SHOULDDQX,0 ACTDQX,0 SHOULDXZF, \
                       0 ACTXZF, 0 SHOULDPGF,0 ACTPGF,0 SHOULDSPF,0 ACTSPF,0 SHOULDLSF,0 ACTLSF,0 SHOULDJJF,0 ACTJJF,\
                       0 SHOULDGZF,0 ACTGZF, 0 SHOULDFIRSTFEES,0 ACTFIRSTFEES,0 SHOULDQTF,0 ACTQTF,0 SHOULDFXJ,0 ACTFXJ,\
                       ifnull(b.ovd_due_fee,a.ovd_due_fee) shouldpenaty,IFNULL(b.ovd_act_fee,a.ovd_act_fee)ACTPENATY, \
                       DATE_FORMAT(IFNULL(b.last_uptime,a.last_uptime),'%Y-%m-%d') update_time, \
                       0 TOTALREDFORFEIT1,0 ACTZQFXJ,0 SHOULDZQFXJ,0 SHOULDCARCOST,0 ACTCARCOST\
                  FROM tio_tn_fms_repay_plan a \
                  LEFT JOIN tio_tn_fms_repay_return b ON a.id = b.plan_id \
                  LEFT JOIN (SELECT repay_id,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE\
                               FROM tio_tn_fms_repay_fee a LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id\
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE') and b.product_type in('car_owner_credit','fast_loan')\
                              GROUP BY repay_id) c ON a.id = c.repay_id\
                  LEFT JOIN (SELECT return_id,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE\
                               FROM tio_tn_fms_repay_return_fee A LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id\
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE') and b.product_type in('car_owner_credit','fast_loan')\
                              GROUP BY return_id) d ON b.id = d.return_id\
                 INNER JOIN tio_tn_fms_loan e ON a.loan_id = e.id\
                 WHERE e.product_type in('car_owner_credit','fast_loan') \
                   and (b.id is not null or a.repay_status not in(1,20))\
                  GROUP BY b.id,e.order_id,a.loan_id,a.current_term,a.terms"
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = "INSERT INTO tb_return_rpt_temp\
                       (APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,SHOULDDELAYINT,TOTALAMT,\
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ, \
                        ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST)\
                SELECT e.order_id,a.loan_id,a.current_term,a.terms,\
                       CASE WHEN a.`repay_status` =10  THEN 2 WHEN a.`repay_status` = 0  THEN -1 ELSE 0 END STATUS, \
                       ifnull(DATE_FORMAT(a.repay_act_time,'%Y-%m-%d'),DATE_FORMAT(a.repay_due_time,'%Y-%m-%d'))PAYDATE,a.ovd_due_interest SHOULDDELAYINT, \
                       0 TOTALAMT,0 ACTCAPITAL, a.repay_due_amount AMT, DATE_FORMAT(a.repay_due_time,'%Y-%m-%d')SHOULDPAYDATE, \
                       a.repay_due_capital SHOULDCAPITAL,a.is_delay ISDELAY,a.repay_due_interest SHOULDINT, \
                       0 ACTMANAGE, 0 ACTDELAYINT, 0 ACTTCF,0 ACTDELAYAMT, 0 ACTSMCSF,0 ACTGPSF,0 ACTTCHF,0 ACTSCF,0 ACTKDF, \
                       0 ACTINT, 0 SHOULDGLF,0 SHOULDTCF,a.prepay_due_fee SHOULDDELAYAMT,c.SHOULDSMCSF SHOULDSMCSF,0 SHOULDGPSF,\
                       c.SHOULDTCHF SHOULDTCHF,0 SHOULDSCF,0 SHOULDKDF,ifnull(c.SHOULDMANAGE,0) SHOULDMANAGE,'3' PAYMODE,a.ovd_days BEOVERDUE, \
                       a.repay_due_amount TOTALSHOULD,a.repay_act_amount TOTALACT, \
                       0 TOTALREDDELAYINT,0 SHOULDKCF,0 ACTKCF ,0 SHOULDDYF,0 ACTDYF,0 SHOULDDQX,0 ACTDQX,0 SHOULDXZF, \
                       0 ACTXZF, 0 SHOULDPGF,0 ACTPGF,0 SHOULDSPF,0 ACTSPF,0 SHOULDLSF,0 ACTLSF,0 SHOULDJJF,0 ACTJJF,\
                       0 SHOULDGZF,0 ACTGZF, 0 SHOULDFIRSTFEES,0 ACTFIRSTFEES,0 SHOULDQTF,0 ACTQTF,0 SHOULDFXJ,0 ACTFXJ, \
                       0 ACTPENATY, DATE_FORMAT(a.last_uptime,'%Y-%m-%d') update_time, \
                       0 TOTALREDFORFEIT1,0 ACTZQFXJ,0 SHOULDZQFXJ,0 SHOULDCARCOST,0 ACTCARCOST\
                  FROM tio_tn_fms_repay_plan a \
                  LEFT JOIN (SELECT repay_id,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,\
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,\
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,\
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE\
                               FROM tio_tn_fms_repay_fee a LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id\
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE') and b.product_type in('car_owner_credit','fast_loan')\
                              GROUP BY repay_id) c ON a.id = c.repay_id\
                 INNER JOIN tio_tn_fms_loan e ON a.loan_id = e.id\
                 WHERE e.product_type in('car_owner_credit','fast_loan') and a.repay_status not in(0,10)\
                 GROUP BY e.order_id,a.loan_id,a.current_term,a.terms"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180531 add 新增超越数
            # 20181020 add 360车贷
            sql = """
                INSERT INTO tb_return_rpt_temp
                       (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,SHOULDDELAYINT,TOTALAMT,
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF,
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,
                        shouldpenaty,ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,
                        actinfoServiceFee,shouldinfoServiceFee)
                 SELECT b.id,e.apply_id,a.loan_id,a.current_term,a.terms,
                       CASE WHEN b.id is not null then 2
                            when b.id is null and a.`repay_status` =10  THEN 2
                            WHEN b.id is null and a.`repay_status` = 0  THEN -1 ELSE 0 END STATUS,
                       DATE_FORMAT(IFNULL(b.repay_act_time,a.repay_due_time),'%Y-%m-%d')PAYDATE,
                       IFNULL(b.ovd_due_interest,a.ovd_due_interest)SHOULDDELAYINT,
                       IFNULL(b.repay_act_amount,a.repay_act_amount) TOTALAMT,
                       IFNULL(b.repay_act_capital,a.repay_act_capital) ACTCAPITAL,
                       IFNULL(b.repay_due_amount,a.repay_due_amount)AMT,
                       DATE_FORMAT(a.repay_due_time,'%Y-%m-%d')SHOULDPAYDATE,
                       IFNULL(b.repay_due_capital,a.repay_due_capital) SHOULDCAPITAL,
                       (CASE WHEN b.id IS NULL THEN a.is_delay WHEN b.id IS NOT NULL AND b.ovd_days>0 THEN 1 ELSE 0 END) ISDELAY,
                       IFNULL(b.repay_due_interest,a.repay_due_interest) SHOULDINT,
                       IFNULL(d.ACTMANAGE,0) ACTMANAGE,
                       IFNULL(b.ovd_act_interest,a.ovd_act_interest)ACTDELAYINT, 0 ACTTCF,
                       IFNULL(b.prepay_act_fee,a.prepay_act_fee)ACTDELAYAMT,
                       IFNULL(d.ACTSMCSF,0) ACTSMCSF,
                       IFNULL(d.ACTGPSF,0) ACTGPSF,
                       IFNULL(d.ACTTCHF,0) ACTTCHF,
                       0 ACTSCF,0 ACTKDF,
                       IFNULL(b.repay_act_interest,a.repay_act_interest)ACTINT,
                       0 SHOULDGLF,0 SHOULDTCF,
                       IFNULL(b.prepay_due_fee,a.prepay_due_fee)SHOULDDELAYAMT,
                       IFNULL(d.SHOULDSMCSF,c.SHOULDSMCSF) SHOULDSMCSF,
                       IFNULL(d.SHOULDGPSF,c.SHOULDGPSF) SHOULDGPSF,
                       IFNULL(d.SHOULDTCHF,c.SHOULDTCHF) SHOULDTCHF,
                       0 SHOULDSCF,0 SHOULDKDF,
                       ifnull(IFNULL(d.SHOULDMANAGE,c.SHOULDMANAGE),0) SHOULDMANAGE,
                       b.repay_mode PAYMODE,IFNULL(b.ovd_days,a.ovd_days) BEOVERDUE,
                       IFNULL(b.repay_due_amount,a.repay_due_amount)TOTALSHOULD,
                       IFNULL(b.repay_act_amount,a.repay_act_amount)TOTALACT,
                       0 TOTALREDDELAYINT,0 SHOULDKCF,0 ACTKCF ,0 SHOULDDYF,0 ACTDYF,0 SHOULDDQX,0 ACTDQX,0 SHOULDXZF,
                       0 ACTXZF, 0 SHOULDPGF,0 ACTPGF,0 SHOULDSPF,0 ACTSPF,0 SHOULDLSF,0 ACTLSF,0 SHOULDJJF,0 ACTJJF,
                       0 SHOULDGZF,0 ACTGZF, 0 SHOULDFIRSTFEES,0 ACTFIRSTFEES,0 SHOULDQTF,0 ACTQTF,0 SHOULDFXJ,0 ACTFXJ,
                       ifnull(b.ovd_due_fee,a.ovd_due_fee) shouldpenaty,
                       IFNULL(b.ovd_act_fee,a.ovd_act_fee)ACTPENATY,
                       DATE_FORMAT(IFNULL(b.last_uptime,a.last_uptime),'%Y-%m-%d') update_time,
                       0 TOTALREDFORFEIT1,0 ACTZQFXJ,0 SHOULDZQFXJ,0 SHOULDCARCOST,0 ACTCARCOST,
                       IFNULL(d.actinfoServiceFee,0) actinfoServiceFee,
                       ifnull(IFNULL(d.shouldinfoServiceFee,c.shouldinfoServiceFee),0) shouldinfoServiceFee
                  FROM tio_tn_fms_repay_plan a
                  LEFT JOIN tio_tn_fms_repay_return b ON a.id = b.plan_id
                  LEFT JOIN (SELECT repay_id,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN act_money END),0) ACTGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN act_money END),0) actinfoServiceFee,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN due_money END),0) shouldGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN due_money END),0) shouldinfoServiceFee
                               FROM tio_tn_fms_repay_fee a LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE','GPS_FEE','INFO_SERVICE_FEE')
                                and b.product_type in('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','360_car_loan')
                              GROUP BY repay_id) c ON a.id = c.repay_id
                  LEFT JOIN (SELECT return_id,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN act_money END),0) ACTGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN act_money END),0) actinfoServiceFee,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN due_money END),0) shouldGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN due_money END),0) shouldinfoServiceFee
                               FROM tio_tn_fms_repay_return_fee A LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE','GPS_FEE','INFO_SERVICE_FEE')
                                and b.product_type in('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','360_car_loan')
                              GROUP BY return_id) d ON b.id = d.return_id
                 INNER JOIN tio_tn_fms_loan e ON a.loan_id = e.id
                 WHERE e.product_type in('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','360_car_loan')
                   and (b.id is not null or a.repay_status not in(1,20))
                  GROUP BY b.id,e.order_id,a.loan_id,a.current_term,a.terms;
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = """
                INSERT INTO tb_return_rpt_temp
                       (APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,SHOULDDELAYINT,TOTALAMT,
                        ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,
                        ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,
                        SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,
                        TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,
                        ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF,
                        ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,
                        ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,
                        actinfoServiceFee,shouldinfoServiceFee)
                SELECT e.apply_id,a.loan_id,a.current_term,a.terms,
                       CASE WHEN a.`repay_status` =10  THEN 2 WHEN a.`repay_status` = 0  THEN -1 ELSE 0 END STATUS,
                       ifnull(DATE_FORMAT(a.repay_act_time,'%Y-%m-%d'),DATE_FORMAT(a.repay_due_time,'%Y-%m-%d'))PAYDATE,a.ovd_due_interest SHOULDDELAYINT,
                       0 TOTALAMT,0 ACTCAPITAL, a.repay_due_amount AMT, DATE_FORMAT(a.repay_due_time,'%Y-%m-%d')SHOULDPAYDATE,
                       a.repay_due_capital SHOULDCAPITAL,a.is_delay ISDELAY,a.repay_due_interest SHOULDINT,
                       0 ACTMANAGE, 0 ACTDELAYINT, 0 ACTTCF,0 ACTDELAYAMT, 0 ACTSMCSF,0 ACTGPSF,0 ACTTCHF,0 ACTSCF,0 ACTKDF,
                       0 ACTINT, 0 SHOULDGLF,0 SHOULDTCF,a.prepay_due_fee SHOULDDELAYAMT,c.SHOULDSMCSF SHOULDSMCSF,c.SHOULDGPSF SHOULDGPSF,
                       c.SHOULDTCHF SHOULDTCHF,0 SHOULDSCF,0 SHOULDKDF,ifnull(c.SHOULDMANAGE,0) SHOULDMANAGE,'3' PAYMODE,a.ovd_days BEOVERDUE,
                       a.repay_due_amount TOTALSHOULD,a.repay_act_amount TOTALACT,
                       0 TOTALREDDELAYINT,0 SHOULDKCF,0 ACTKCF ,0 SHOULDDYF,0 ACTDYF,0 SHOULDDQX,0 ACTDQX,0 SHOULDXZF,
                       0 ACTXZF, 0 SHOULDPGF,0 ACTPGF,0 SHOULDSPF,0 ACTSPF,0 SHOULDLSF,0 ACTLSF,0 SHOULDJJF,0 ACTJJF,
                       0 SHOULDGZF,0 ACTGZF, 0 SHOULDFIRSTFEES,0 ACTFIRSTFEES,0 SHOULDQTF,0 ACTQTF,0 SHOULDFXJ,0 ACTFXJ,
                       0 ACTPENATY, DATE_FORMAT(a.last_uptime,'%Y-%m-%d') update_time,
                       0 TOTALREDFORFEIT1,0 ACTZQFXJ,0 SHOULDZQFXJ,0 SHOULDCARCOST,0 ACTCARCOST,
                       IFNULL(c.actinfoServiceFee,0) actinfoServiceFee,
                       ifnull(c.shouldinfoServiceFee,0) shouldinfoServiceFee
                  FROM tio_tn_fms_repay_plan a
                  LEFT JOIN (SELECT repay_id,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN act_money END),0) ACTSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN act_money END),0) ACTTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN act_money END),0)ACTMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN act_money END),0) ACTGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN act_money END),0) actinfoServiceFee,
                                    IFNULL(SUM(CASE WHEN label='VISIT_FEE' THEN due_money END),0) SHOULDSMCSF,
                                    IFNULL(SUM(CASE WHEN label='TOW_CAR_FEE' THEN due_money END),0) SHOULDTCHF,
                                    IFNULL(SUM(CASE WHEN label='ACCOUNT_MGMT_FEE' THEN due_money END),0) SHOULDMANAGE,
                                    IFNULL(SUM(CASE WHEN label='GPS_FEE' THEN due_money END),0) shouldGPSF,
                                    IFNULL(SUM(CASE WHEN label='INFO_SERVICE_FEE' THEN due_money END),0) shouldinfoServiceFee
                               FROM tio_tn_fms_repay_fee a
                               LEFT JOIN tio_tn_fms_repay_fee_config b on a.config_id = b.id
                              WHERE b.label IN('VISIT_FEE','TOW_CAR_FEE','ACCOUNT_MGMT_FEE','GPS_FEE','INFO_SERVICE_FEE')
                               and b.product_type in('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','360_car_loan')
                              GROUP BY repay_id) c ON a.id = c.repay_id
                 INNER JOIN tio_tn_fms_loan e ON a.loan_id = e.id
                 WHERE e.product_type in('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','360_car_loan')
                   and a.repay_status not in(0,10)
                 GROUP BY e.order_id,a.loan_id,a.current_term,a.terms
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 删除回款表中水畔、车主信用贷、社保贷 的记录
            sql = "DELETE a FROM tb_return_rpt a WHERE a.ENDDATE is null"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 回款表删除在临时表中数据
            sql = "DELETE a FROM tb_return_rpt a ,tb_return_rpt_temp b \
                 WHERE a.returnDetailID = b.returnDetailID and a.ENDDATE is not null and b.ENDDATE is not null"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 回款表删除在临时表中数据
            sql = "DELETE a FROM tb_return_rpt a ,tb_return_rpt_mid_temp b WHERE a.returnDetailID = b.id and a.ENDDATE is not null"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 插入更新日期是昨天到今天的
            sql_db = "insert into tb_return_rpt  \
                         (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,TOTALAMT,\
                            ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                            ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                            SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                            TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                            ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                            ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,SHOULDPENATY, \
                            ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,\
                            actinfoServiceFee,shouldinfoServiceFee) \
                  select returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,TOTALAMT,\
                            ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                            ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                            SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                            TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                            ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                            ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,SHOULDPENATY, \
                            ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,\
                            actinfoServiceFee,shouldinfoServiceFee \
                    from tb_return_rpt_temp"
            r = rpt_db.query(sql_db)
            rpt_db.commit()

            # 插入多出来的历史数据
            sql_db = "insert into tb_return_rpt  \
                         (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,TOTALAMT,\
                            ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                            ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                            SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                            TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                            ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                            ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,SHOULDPENATY, \
                            ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,\
                            actinfoServiceFee,shouldinfoServiceFee) \
                     select a.id,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,TOTALAMT,\
                            ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,ACTDELAYINT,ACTTCF,\
                            ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,\
                            SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,\
                            TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,\
                            ACTDQX,SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF, \
                            ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,SHOULDFXJ,ACTFXJ,SHOULDPENATY, \
                            ACTPENATY,update_time,TOTALREDFORFEIT1,ACTZQFXJ,SHOULDZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,\
                            actinfoServiceFee,shouldinfoServiceFee \
                    from tb_return_rpt_mid_temp a\
                    left join tio_xd_sys_bankaccount b on a.ACCOUNTID=b.id;"
            r = rpt_db.query(sql_db)
            rpt_db.commit()

            # 删除回款表中多余的记录
            sql = "DELETE a FROM tb_return_rpt a \
                 WHERE not EXISTS(SELECT 1 FROM tio_xd_lf_returndetailall b WHERE a.returnDetailID = b.id) \
                   and a.ENDDATE is not null"
            print sql
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 每月5号备份上月还款明细
            if today.day == 5:
                sql = "delete from tb_return_rpt_bak  \
                      WHERE paydate >=DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL EXTRACT(DAY FROM NOW())-1 DAY),INTERVAL 1 MONTH) \
                        AND paydate <=DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL EXTRACT(DAY FROM NOW()) DAY),INTERVAL 0 MONTH)"
                r = rpt_db.query(sql)
                rpt_db.commit()

                sql = "INSERT INTO tb_return_rpt_bak \
                            (returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,\
                            ACTDELAYINT,ACTTCF,ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,\
                            SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,TOTALAMT,TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,ACTDQX,\
                            SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF,ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,\
                            SHOULDFXJ,ACTFXJ,ACTPENATY,update_time,TOTALREDFORFEIT1,SHOULDZQFXJ,ACTZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,actinfoServiceFee,shouldinfoServiceFee)\
                     SELECT returnDetailID,APPLYID,LOANID,PAYPHASES,TOTALPHASES,STATUS,PAYDATE,ENDDATE,SHOULDDELAYINT,ACTCAPITAL,AMT,SHOULDPAYDATE,SHOULDCAPITAL,ISDELAY,SHOULDINT,ACTMANAGE,\
                            ACTDELAYINT,ACTTCF,ACTDELAYAMT,ACTSMCSF,ACTGPSF,ACTTCHF,ACTSCF,ACTKDF,ACTINT,SHOULDGLF,SHOULDTCF,SHOULDDELAYAMT,SHOULDSMCSF,SHOULDGPSF,SHOULDTCHF,\
                            SHOULDSCF,SHOULDKDF,SHOULDMANAGE,PAYMODE,BEOVERDUE,TOTALAMT,TOTALSHOULD,TOTALACT,TOTALREDDELAYINT,SHOULDKCF,ACTKCF,SHOULDDYF,ACTDYF,SHOULDDQX,ACTDQX,\
                            SHOULDXZF,ACTXZF,SHOULDPGF,ACTPGF,SHOULDSPF,ACTSPF,SHOULDLSF,ACTLSF,SHOULDJJF,ACTJJF,SHOULDGZF,ACTGZF,SHOULDFIRSTFEES,ACTFIRSTFEES,SHOULDQTF,ACTQTF,\
                            SHOULDFXJ,ACTFXJ,ACTPENATY,update_time,TOTALREDFORFEIT1,SHOULDZQFXJ,ACTZQFXJ,SHOULDCARCOST,ACTCARCOST,bankaccount,actinfoServiceFee,shouldinfoServiceFee\
                       FROM tb_return_rpt a\
                      WHERE a.paydate >=DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL EXTRACT(DAY FROM NOW())-1 DAY),INTERVAL 1 MONTH) \
                        AND a.paydate <=DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL EXTRACT(DAY FROM NOW()) DAY),INTERVAL 0 MONTH)"
                r = rpt_db.query(sql)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]回款表tb_return_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 分公司月度目标表-tb_sitetarget_rpt
def trans_sitetarget():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]分公司月度目标表tb_sitetarget_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '分公司月度目标表tb_sitetarget_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '分公司月度目标表tb_sitetarget_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]分公司月度目标表tb_sitetarget_rpt->trytimes:', trytimes
        try:
            clear_table('tb_sitetarget_rpt_temp')
            sql = "INSERT INTO tb_sitetarget_rpt_temp \
                       (ID,SITECODE,TARGETMONTH,MINITARGET,REWARDTARGET,STATUS,PRODUCTCODE)\
                select ID,SITECODE,TARGETMONTH,MINITARGET,REWARDTARGET,STATUS,PRODUCTCODE \
                  from tio_xd_sys_sitetarget"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_sitetarget_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]分公司月度目标表tb_sitetarget_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_sitetarget_rpt')
                sql_db = "insert into tb_sitetarget_rpt  \
                             (ID,SITECODE,TARGETMONTH,MINITARGET,REWARDTARGET,STATUS,PRODUCTCODE) \
                      select ID,SITECODE,TARGETMONTH,MINITARGET,REWARDTARGET,STATUS,PRODUCTCODE \
                        from tb_sitetarget_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]分公司月度目标表tb_sitetarget_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 数据词典tb_dist_rpt
def trans_dist():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]数据词典tb_dist_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '数据词典tb_dist_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '数据词典tb_dist_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]数据词典tb_dist_rpt->trytimes:', trytimes
        try:
            clear_table('tb_dist_rpt_temp')
            sql = "INSERT INTO tb_dist_rpt_temp \
                       (ID,PID,DATATYPE,DATACODE,DATAVALUE,SORTNO,STATUS,DATADESC)\
                select ID,PID,DATATYPE,DATACODE,DATAVALUE,SORTNO,STATUS,DATADESC from tio_xd_sys_dist"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_dist_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]数据词典tb_dist_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_dist_rpt')
                sql_db = "insert into tb_dist_rpt  \
                             (ID,PID,DATATYPE,DATACODE,DATAVALUE,SORTNO,STATUS,DATADESC) \
                      select ID,PID,DATATYPE,DATACODE,DATAVALUE,SORTNO,STATUS,DATADESC \
                        from tb_dist_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]数据词典tb_dist_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 其它费用tb_otherfee_rpt
def trans_otherfee():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]其它费用tb_otherfee_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '其它费用tb_otherfee_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '其它费用tb_otherfee_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]其它费用tb_otherfee_rpt->trytimes:', trytimes
        try:
            clear_table('tb_otherfee_rpt_temp')
            sql = "INSERT INTO tb_otherfee_rpt_temp \
                       (ID,APPLYID,SFITEM,SFAMT,paymentdetailid,SFTYPE)\
                select ID,APPLYID,SFITEM,SFAMT,paymentdetailid,SFTYPE from tio_xd_lm_otherfee"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_otherfee_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]其它费用tb_otherfee_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_otherfee_rpt')
                sql_db = "insert into tb_otherfee_rpt  \
                             (ID,APPLYID,SFITEM,SFAMT,paymentdetailid,SFTYPE) \
                      select ID,APPLYID,SFITEM,SFAMT,paymentdetailid,SFTYPE \
                        from tb_otherfee_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]其它费用tb_otherfee_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 贷款费用表tb_fee_rpt
def trans_fee():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]贷款费用表tb_fee_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '贷款费用表tb_fee_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '贷款费用表tb_fee_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]贷款费用表tb_fee_rpt->trytimes:', trytimes
        try:
            clear_table('tb_fee_rpt_temp')
            sql = "INSERT INTO tb_fee_rpt_temp \
                       (APPLYID,SFITEM,SFAMT,SFTYPE,paymentdetailid)\
                select APPLYID,SFITEM,SFAMT,1,paymentdetailid from tio_xd_lm_dsmessage union all\
                select APPLYID,SFITEM,SFAMT,2,paymentdetailid from tio_xd_lm_otherfee"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_fee_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]贷款费用表tb_fee_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_fee_rpt')
                sql_db = "insert into tb_fee_rpt  \
                             (APPLYID,SFITEM,SFAMT,SFTYPE,paymentdetailid) \
                      select APPLYID,SFITEM,SFAMT,SFTYPE,paymentdetailid \
                        from tb_fee_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except IOError:  # 读不到数据的错误
            traceback.print_exc()
            continue
        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            continue
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]贷款费用表tb_fee_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 审批流程表tb_applyflow_rpt
def trans_applyflow():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]审批流程表tb_applyflow_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '审批流程表tb_applyflow_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '审批流程表tb_applyflow_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]审批流程表tb_applyflow_rpt->trytimes:', trytimes
        try:
            clear_table('tb_applyflow_rpt_temp')
            sql = "INSERT INTO tb_applyflow_rpt_temp \
                       (ID, APPLYID, SIGNCODE, FLOWSIGN, FLOWNAME, ACTIONDATE, ACTIONBY, RESULT, STATUS, SITENO, SITENAME, CONTRACTNO)\
                select t.ID,t.APPLYID,t.SIGNCODE,t.FLOWSIGN,IF (t.FLOWNAME = \'客户取消\',CONCAT(\'客户取消\',\'-客户通过APP取消\'),t.FLOWNAME),\
                       t.ACTIONDATE,IF (e.EMPNAME IS NULL,\' \',e.EMPNAME) ACTIONBY,t.RESULT,t.`STATUS`,b.SITENO,c.SITENAME,d.CONTRACTNO \
                  from tio_xd_lb_applyflow t\
                  left join tio_xd_lb_applyinfo b on t.APPLYID = b.ID\
                  left join tio_xd_sys_site c on c.SITECODE=b.SITENO\
                  left join tio_xd_lm_contract d on b.ID=d.APPLYID \
                  LEFT JOIN tio_xd_sys_user u on t.ACTIONBY = u.USERNAME\
                  LEFT JOIN tio_xd_sys_emp e on u.EMPID = e.ID\
                 where t.`STATUS`=2"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_applyflow_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            # 审批流程表删除在临时表中数据
            sql = "DELETE a FROM tb_applyflow_rpt a ,tb_applyflow_rpt_temp b WHERE a.id = b.id"
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql_db = "insert into tb_applyflow_rpt  \
                         (ID, APPLYID, SIGNCODE, FLOWSIGN, FLOWNAME, ACTIONDATE, ACTIONBY, RESULT, STATUS, SITENO, SITENAME, CONTRACTNO) \
                  select ID, APPLYID, SIGNCODE, FLOWSIGN, FLOWNAME, ACTIONDATE, ACTIONBY, RESULT, STATUS, SITENO, SITENAME, CONTRACTNO \
                    from tb_applyflow_rpt_temp"
            r = rpt_db.query(sql_db)
            rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]审批流程表tb_applyflow_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 贷款申请审核表tb_applyaudit_rpt
def trans_applyaudit():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]贷款申请审核表tb_applyaudit_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '贷款申请审核表tb_applyaudit_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '贷款申请审核表tb_applyaudit_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]贷款申请审核表tb_applyaudit_rpt->trytimes:', trytimes
        try:
            clear_table('tb_applyaudit_rpt_temp')
            sql = "INSERT INTO tb_applyaudit_rpt_temp \
                       (ID,APPLYID,AUDITLEVEL,SERIALNUMBER,AUDITAMT,RECAMT,PHASES,RATE,MANAGEFEES,FEE,\
                        AUDITRESULT,SUGGESTION,AUDITSTATUS, AUDITBY,AUDITTIME,BACKFLOW,RECID,BACKFLOWID,\
                        DERATIO,DRRATIO,LLRATIO,CONTRACTTEMPTYPE,DKFS,HKFS,PRODUCTTYPE)\
                select a.ID,APPLYID,AUDITLEVEL,SERIALNUMBER,AUDITAMT,RECAMT,PHASES,a.RATE,MANAGEFEES,FEE,AUDITRESULT,SUGGESTION,AUDITSTATUS,\
                       a.AUDITBY,a.AUDITTIME,BACKFLOW,RECID,BACKFLOWID,DERATIO,DRRATIO,LLRATIO,a.CONTRACTTEMPTYPE,a.DKFS,a.HKFS,c.PRODUCTTYPE\
                  from tio_xd_lb_applyaudit a \
                  LEFT JOIN tio_xd_lb_applyinfo b ON a.APPLYID = b.ID \
                  LEFT JOIN tio_xd_sys_product c ON b.PRODUCTS = c.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_applyaudit_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]贷款申请审核表tb_applyaudit_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_applyaudit_rpt')
                sql_db = "insert into tb_applyaudit_rpt  \
                             (ID,APPLYID,AUDITLEVEL,SERIALNUMBER,AUDITAMT,RECAMT,PHASES,RATE,MANAGEFEES,FEE,\
                             AUDITRESULT,SUGGESTION,AUDITSTATUS, AUDITBY,AUDITTIME,BACKFLOW,RECID,BACKFLOWID,\
                             DERATIO,DRRATIO,LLRATIO,CONTRACTTEMPTYPE,DKFS,HKFS,PRODUCTTYPE) \
                      select ID,APPLYID,AUDITLEVEL,SERIALNUMBER,AUDITAMT,RECAMT,PHASES,RATE,MANAGEFEES,FEE,\
                             AUDITRESULT,SUGGESTION,AUDITSTATUS, AUDITBY,AUDITTIME,BACKFLOW,RECID,BACKFLOWID,\
                             DERATIO,DRRATIO,LLRATIO,CONTRACTTEMPTYPE,DKFS,HKFS,PRODUCTTYPE \
                        from tb_applyaudit_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]贷款申请审核表tb_applyaudit_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 联合贷产品表数据tb_unified_dependent
def trans_unified_dependent():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]联合贷产品表数据tb_unified_dependent开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '联合贷产品表数据tb_unified_dependent重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '联合贷产品表数据tb_unified_dependent重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]联合贷产品表数据tb_unified_dependent->trytimes:', trytimes
        try:
            clear_table('tb_unified_dependent_temp')
            sql = "INSERT INTO tb_unified_dependent_temp \
                       (applyid,dependent_applyid,createtime,last_modify_time)\
                select applyid,dependent_applyid,create_time,DATE_FORMAT(last_modify_time, '%Y-%m-%d') from tio_xd_unified_dependent"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select applyid from tb_unified_dependent_temp"
            result_r = rpt_db.querySql(sql, field=['applyid'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]联合贷产品表数据tb_unified_dependent_temp数据异常! 结束时间：%s，运行时间：%s" % (
                end_time, end_time - start_time)
                return
            else:
                clear_table('tb_unified_dependent')
                sql_db = "insert into tb_unified_dependent  \
                             (applyid,dependent_applyid,createtime,last_modify_time) \
                      select applyid,dependent_applyid,createtime,last_modify_time \
                        from tb_unified_dependent_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]联合贷产品表数据tb_unified_dependent结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 费用减免表tb_costreduction_detail
def trans_costreduction_detail():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]费用减免表tb_costreduction_detail开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '费用减免表tb_costreduction_detail重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '费用减免表tb_costreduction_detail重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]费用减免表tb_costreduction_detail->trytimes:', trytimes
        try:
            clear_table('tb_costreduction_detail_temp')
            # 车贷费用减免数据 添加担保公司字段
            sql = """
              INSERT INTO tb_costreduction_detail_temp
                       (SITENO, CUSTNAME, APPLYDATE, CONTRACTNO, productname, coopName, REASONTYPE, ISSELLCARS,ISLAWSUIT, FEESTYPENAME, SHOULDRECEIVE,
                        ACTUALRECEIVE, REDUCTION,PHASES, APPLYBY, FINALJUDGEMENT, FINALJUDGEDATE, ISDIRECT, STATUS, REMARK,applyid,orgid,corpName)
                select d.SITENO,e.CUSTNAME,a.applydate,c.CONTRACTNO,p.DATAVALUE productname,case when d.PRODUCTTYPE = 'P019' then  '蚂蚁借呗' end coopName,
                      (case when a.reasontype = 'custReason' then '客户原因' when a.reasontype = 'persReason' then '人为原因' when a.reasontype = 'systReason' then '系统原因'
                            when a.reasontype='ruleReason' then '政策原因' when a.reasontype='PropFree' then '道具减免' when a.reasontype='otherReason' then '其他原因' end) REASONTYPE,
                      i.is_maiche ISSELLCARS,i.is_susong ISLAWSUIT,b.FEESTYPENAME FEESTYPENAME,
                      (case when b.FEESTYPENAME = '逾期利息' then SHOULDDELAYINT when b.FEESTYPENAME = '上门催收费' then SHOULDSMCSF when b.FEESTYPENAME = '违约金' then SHOULDDELAYAMT
                      when b.FEESTYPENAME = '利息' then SHOULDINT when b.FEESTYPENAME = '管理费' then SHOULDMANAGE when b.FEESTYPENAME = '拖车费' then SHOULDTCHF
                      when b.FEESTYPENAME = '停车费' then SHOULDTCF when b.FEESTYPENAME = 'GPS费' then SHOULDGPSF when b.FEESTYPENAME = '赎车费' then SHOULDSCF
                      when b.FEESTYPENAME = '风险金' then SHOULDFXJ when b.FEESTYPENAME = '逾期违约金' then SHOULDPENATY when b.FEESTYPENAME = '展期风险金' then SHOULDZQFXJ
                      when b.FEESTYPENAME = '手续费' then SHOULDFIRSTFEES when b.FEESTYPENAME = '其他费' then SHOULDQTF when b.FEESTYPENAME = '本金' then SHOULDCAPITAL
                      else 0 end) SHOULDRECEIVE,
                      (case when b.FEESTYPENAME = '逾期利息' then ACTDELAYINT when b.FEESTYPENAME = '上门催收费' then ACTSMCSF when b.FEESTYPENAME = '违约金' then ACTDELAYAMT
                      when b.FEESTYPENAME = '利息' then ACTINT when b.FEESTYPENAME = '管理费' then ACTMANAGE when b.FEESTYPENAME = '拖车费' then ACTTCHF
                      when b.FEESTYPENAME = '停车费' then ACTTCF when b.FEESTYPENAME = 'GPS费' then ACTGPSF when b.FEESTYPENAME = '赎车费' then ACTSCF when b.FEESTYPENAME = '风险金' then ACTFXJ
                      when b.FEESTYPENAME = '逾期违约金' then ACTPENATY when b.FEESTYPENAME = '展期风险金' then ACTZQFXJ when b.FEESTYPENAME = '手续费' then ACTFIRSTFEES
                      when b.FEESTYPENAME = '其他费' then ACTQTF when b.FEESTYPENAME = '本金' then ACTCAPITAL
                      else 0 end) ACTUALRECEIVE, b.amt REDUCTION,b.phases phases,a.APPLYBY APPLYBY,
                      f.SUBMITUSER FINALJUDGEMENT,f.AUDITTIME FINALJUDGEDATE,if(d.DIRECTSELLER is not null,1,0) ISDIRECT, a.status STATUS,a.remark REMARK,d.APPLYID,g.orgid,
                      j.corp_Name
                  from tio_xd_lf_specialfeesfree a
                  left join tio_xd_lf_specialfeesfreesub b on a.id = b.PID
                  left join (select id,applyid,contractno from tio_xd_lm_payment group by contractno) c on a.LOANID = c.id
                  left join tb_applyinfo_rpt d on c.APPLYID = d.APPLYID
                  LEFT JOIN tb_dist_rpt p on d.PRODUCTTYPE = p.DATACODE and p.DATATYPE = 'PT001'
                  LEFT JOIN tio_xd_sys_emp_his g ON d.SALESMAN = g.id AND d.AUDITTIME>=g.starttime AND d.AUDITTIME<= g.endtime
                  left join tio_xd_lb_custinfo e on c.APPLYID = e.APPLYID
                  left join (select a.loanid,a.PAYPHASES,max(SHOULDDELAYINT)SHOULDDELAYINT,max(SHOULDSMCSF)SHOULDSMCSF,max(SHOULDDELAYAMT)SHOULDDELAYAMT,max(SHOULDINT)SHOULDINT,
										  max(SHOULDMANAGE)SHOULDMANAGE,max(SHOULDTCHF)SHOULDTCHF,max(SHOULDTCF)SHOULDTCF, max(SHOULDGPSF)SHOULDGPSF,max(SHOULDSCF)SHOULDSCF,max(SHOULDFXJ)SHOULDFXJ,
										  max(SHOULDPENATY)SHOULDPENATY,max(SHOULDZQFXJ)SHOULDZQFXJ,max(SHOULDFIRSTFEES)SHOULDFIRSTFEES,max(SHOULDQTF)SHOULDQTF,max(SHOULDCAPITAL)SHOULDCAPITAL,
										  sum(ACTDELAYINT) ACTDELAYINT, sum(ACTSMCSF) ACTSMCSF, sum(ACTDELAYAMT) ACTDELAYAMT,sum(ACTINT) ACTINT,
										  sum(ACTMANAGE) ACTMANAGE,sum(ACTTCHF) ACTTCHF,sum(ACTTCF) ACTTCF,sum(ACTGPSF) ACTGPSF,sum(ACTSCF) ACTSCF,sum(ACTFXJ) ACTFXJ,
										  sum(ACTPENATY) ACTPENATY,sum(ACTZQFXJ) ACTZQFXJ,sum(ACTFIRSTFEES) ACTFIRSTFEES,sum(ACTQTF) ACTQTF,sum(ACTcapital) ACTcapital
								    from tb_return_rpt a, tio_xd_lf_specialfeesfree b
                              where a.ENDDATE is not null and a.loanid=b.loanid and a.PAYPHASES=b.phases
                              group by a.loanid,a.PAYPHASES) h on h.LOANID = a.LOANID  and h.PAYPHASES = a.phases
                  left join (select * from (select pid,SUBMITUSER,AUDITTIME from tio_xd_lf_specialaudit where SPECIALTYPE = 5 ORDER BY id desc) a group by pid) f on a.id = f.PID
                  left join (select PAYMENTID,if(situation =2,1,0) is_maiche,if(situation=6,1,0) is_susong from tio_xd_la_urge group by PAYMENTID) i on a.LOANID = i.PAYMENTID
                  left join tio_xd_lm_guarantee_amt j on d.applyid = j.apply_id
                  where a.status in (3,4)
                   and b.amt > 0
                   and b.FEESTYPENAME in ('逾期利息','上门催收费','违约金','利息','管理费','拖车费','停车费','GPS费','赎车费','风险金','逾期违约金','展期风险金','手续费','其他费','本金');"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180706 add 信用贷费用减免
            # 20190110  add 新增超数分公司
            sql = """
              insert into tb_costreduction_detail_temp(SITENO, CUSTNAME, APPLYDATE, CONTRACTNO, REASONTYPE, FEESTYPENAME, SHOULDRECEIVE, ACTUALRECEIVE,
                      REDUCTION,PHASES, APPLYBY, FINALJUDGEMENT, FINALJUDGEDATE, ISDIRECT, STATUS, REMARK,applyid, productname, coopName,cyssitename)
              select ifnull(a.site_no,'-') SITENO, a.cust_name, date(b.apply_date), a.contract_no,
                     case c.reason_type when 1 then '系统原因' when 2 then '客户原因' when 3 then '人为原因' when 4 then '政策原因' end as REASONTYPE,
                     case e.label when 'CAPITAL' then '本金' when 'INTEREST' then '利息' when 'ACCOUNT_MGMT_FEE' then '管理费' when 'OVD_INTEREST' then '逾期利息'
                                  when 'prepay_fee' then '提前还款违约金' when 'PROCESS_FEE' then '手续费'
                                  end as FEESTYPENAME,
                     case e.label when 'CAPITAL' then f.SHOULDCAPITAL when 'INTEREST' then f.SHOULDINT when 'ACCOUNT_MGMT_FEE' then f.SHOULDMANAGE
                                  when 'OVD_INTEREST' then f.SHOULDDELAYINT when 'prepay_fee' then f.SHOULDDELAYAMT when 'PROCESS_FEE' then f.SHOULDFIRSTFEES else 0
                                  end as SHOULDRECEIVE,
                     case e.label when 'CAPITAL' then f.ACTCAPITAL when 'INTEREST' then f.ACTINT when 'ACCOUNT_MGMT_FEE' then f.ACTMANAGE
                                  when 'OVD_INTEREST' then f.ACTDELAYINT when 'prepay_fee' then f.ACTDELAYAMT when 'PROCESS_FEE' then f.ACTFIRSTFEES else 0
                                  end as ACTUALRECEIVE,
                     d.money REDUCTION,c.term,b.addname,b.audit_user,date(b.last_uptime),0 ISDIRECT,if(b.audit_status='PASS',4,3) STATUS,
                     c.remark,a.order_id,a.product_name,
                     case when a.product_type like 'cys%' then '超越数'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'gd' THEN '个贷'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'db' THEN '袋吧'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'huarun' THEN '华润'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'cys' THEN '超越数'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = '360' THEN '360'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'xxfw' THEN '服务号'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'iqiyi' THEN '爱奇艺'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'fxb' THEN '房信宝'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'haotian' THEN '浩天车信贷'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'XD-BBW' THEN '贝贝网'
                          WHEN a.product_type = 'fast_loan' and a.`org_dept` = 'XD_MZ' THEN '魅族'
                      end coopName,
                     case when a.product_type like 'cys%' then a.site_name end cyssitename
              from tio_tn_fms_loan a
              join tio_tn_fms_fee_free_apply b on a.id = b.loan_id
              left join tio_tn_fms_fee_free_plan c on b.id = c.free_id
              left join tio_tn_fms_fee_free_detail d on c.id = d.free_plan_id
              left join tio_tn_fms_repay_fee_config e on d.config_id = e.id
              left join(select r.APPLYID,r.PAYPHASES,r.SHOULDCAPITAL,r.SHOULDINT,r.SHOULDMANAGE,r.SHOULDDELAYINT,r.SHOULDDELAYAMT,r.SHOULDFIRSTFEES,
                               sum(r.ACTCAPITAL) ACTCAPITAL,sum(r.ACTINT) ACTINT,sum(r.ACTMANAGE) ACTMANAGE,sum(r.ACTDELAYINT) ACTDELAYINT,
                               sum(r.ACTDELAYAMT) ACTDELAYAMT,sum(r.ACTFIRSTFEES) ACTFIRSTFEES
                          from tb_return_rpt r, tb_applyinfo_rpt a
                         where r.APPLYID = a.APPLYID
                           and a.PRODUCTTYPE in ('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','fast_loan','car_owner_credit')
                         group by r.APPLYID,r.PAYPHASES)f on a.order_id = f.APPLYID and c.term = f.PAYPHASES
              where a.product_type in ('cys_fang','cys_social_security','cys_policy','cys_accumulation_loan','fast_loan','car_owner_credit')
                and b.audit_status IN ('PASS','REJECT')
                and e.label in ('CAPITAL','INTEREST','ACCOUNT_MGMT_FEE','OVD_INTEREST','prepay_fee','PROCESS_FEE');
            """
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 20180421 add
            # 更新每笔单的未还本金
            # sql = "UPDATE tb_costreduction_detail_temp aa ,\
            #            (SELECT b.APPLYID,SUM(b.SHOULDCAPITAL)SHOULDCAPITAL \
            #               FROM tb_return_rpt b,tb_costreduction_detail_temp a \
            #              WHERE b.applyid = a.applyid AND b.STATUS<>2 GROUP BY b.applyid) bb\
            #        SET aa.noReturnCapital = IFNULL(bb.SHOULDCAPITAL,0)\
            #      WHERE aa.APPLYID = bb.applyid"
            # r = rpt_db.query()
            # rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_costreduction_detail_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]费用减免表tb_costreduction_detail_temp数据异常! 结束时间：%s，运行时间：%s" % (
                end_time, end_time - start_time)
                return
            else:
                clear_table('tb_costreduction_detail')
                sql_db = "insert into tb_costreduction_detail  \
                             (SITENO, CUSTNAME, APPLYDATE, CONTRACTNO, REASONTYPE, ISSELLCARS,ISLAWSUIT, FEESTYPENAME, SHOULDRECEIVE, ACTUALRECEIVE,\
                             REDUCTION,PHASES, APPLYBY, FINALJUDGEMENT, FINALJUDGEDATE, ISDIRECT, STATUS, REMARK,applyid,orgid,productname,coopName,\
                             corpName,cyssitename) \
                      select SITENO, CUSTNAME, APPLYDATE, CONTRACTNO, REASONTYPE, ISSELLCARS,ISLAWSUIT, FEESTYPENAME, SHOULDRECEIVE, ACTUALRECEIVE,\
                             REDUCTION,PHASES, APPLYBY, FINALJUDGEMENT, FINALJUDGEDATE, ISDIRECT, STATUS, REMARK,applyid,orgid,productname,coopName,\
                             corpName,cyssitename \
                        from tb_costreduction_detail_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]费用减免表tb_costreduction_detail结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 客户信息表tb_custinfo_rpt
def trans_custinfo():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]客户信息表tb_custinfo_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '客户信息表tb_custinfo_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '客户信息表tb_custinfo_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]客户信息表tb_custinfo_rpt->trytimes:', trytimes
        try:
            clear_table('tb_custinfo_rpt_temp')
            sql = "INSERT INTO tb_custinfo_rpt_temp \
                       (ID,APPLYID,CUSTNAME,MARRY,DOCTYPE,DOCNO,BIRTHPLACE,HOUSEHOLD,CHILDNUM,INDUSTRY,DUTIES,\
                        SALARY,GYRS,ADDR,COMEHEREDATE,LIVETIME,MOBILE,MOBILE2,MOBILE3,GENDER,HOUSETPYE,OPERATETIME,\
                        REGISTERCAPITAL,ONWER,BUYDATE,GPSINSTALLMAN,AGE,DLN)\
                select a.ID,a.APPLYID,a.CUSTNAME,MARRY,DOCTYPE,DOCNO,BIRTHPLACE,HOUSEHOLD,\
                       CHILDNUM,b.INDUSTRY,b.DUTIES,b.SALARY,b.GYRS,c.ADDR,\
                       if(COMEYEAR='-1' or COMEMONTH='-1','', CONCAT(COMEYEAR,CONCAT('-',COMEMONTH))),LIVETIME,MOBILE,MOBILE2,MOBILE3,GENDER,\
                       d.HOUSETPYE,b.OPERATETIME,b.REGISTERCAPITAL,c.ONWER,e.BUYDATE,e.GPSINSTALLMAN ,a.AGE,a.DLN\
                  FROM tio_xd_lb_custinfo a \
                  LEFT JOIN tio_xd_lb_profession b on b.CUSTID=a.ID \
                  LEFT JOIN tio_xd_lb_estateinfo c on c.CUSTID=a.ID \
                  LEFT JOIN tio_xd_lb_houseinfo d on d.CUSTID=a.ID \
                  LEFT JOIN (select m.* \
                               from tio_xd_lb_carmessage m ,\
                                    (select max(n.ID)ID from tio_xd_lb_carmessage n group by n.CUSTID)bb\
                              where m.id = bb.id) e on e.CUSTID=a.ID \
                 group by a.ID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_custinfo_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]客户信息表tb_custinfo_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_custinfo_rpt')
                sql_db = "insert into tb_custinfo_rpt  \
                             (ID,APPLYID,CUSTNAME,MARRY,DOCTYPE,DOCNO,BIRTHPLACE,HOUSEHOLD,CHILDNUM,INDUSTRY,DUTIES,\
                             SALARY,GYRS,ADDR,COMEHEREDATE,LIVETIME,MOBILE,MOBILE2,MOBILE3,GENDER,HOUSETPYE,OPERATETIME,\
                             REGISTERCAPITAL,ONWER,BUYDATE,GPSINSTALLMAN,AGE,DLN) \
                      select ID,APPLYID,CUSTNAME,MARRY,DOCTYPE,DOCNO,BIRTHPLACE,HOUSEHOLD,CHILDNUM,INDUSTRY,DUTIES,\
                             SALARY,GYRS,ADDR,COMEHEREDATE,LIVETIME,MOBILE,MOBILE2,MOBILE3,GENDER,HOUSETPYE,OPERATETIME,\
                             REGISTERCAPITAL,ONWER,BUYDATE,GPSINSTALLMAN,AGE,DLN \
                        from tb_custinfo_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]客户信息表tb_custinfo_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 用户账号表tb_custaccount_rpt
def trans_custaccount():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]用户账号表tb_custaccount_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '用户账号表tb_custaccount_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '用户账号表tb_custaccount_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]用户账号表tb_custaccount_rpt->trytimes:', trytimes
        try:
            clear_table('tb_custaccount_rpt_temp')
            sql = "INSERT INTO tb_custaccount_rpt_temp \
                       (ID,APPLYID,CONTRACTID,CUSTNAME,CUSTNO,PAYMENT,BANKACCOUNT,BANK,BANKNO,ACCOUNTTYPE,ISDEFAULT,BANKTYPE,BANKBRANCH)\
                select ID,APPLYID,CONTRACTID,CUSTNAME,CUSTNO,PAYMENT,BANKACCOUNT,BANK,BANKNO,ACCOUNTTYPE,ISDEFAULT,BANKTYPE,BANKBRANCH\
                  from tio_xd_lm_custaccount"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_custaccount_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]用户账号表tb_custaccount_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_custaccount_rpt')
                sql_db = "insert into tb_custaccount_rpt  \
                             (ID,APPLYID,CONTRACTID,CUSTNAME,CUSTNO,PAYMENT,BANKACCOUNT,BANK,BANKNO,ACCOUNTTYPE,ISDEFAULT,BANKTYPE,BANKBRANCH) \
                      select ID,APPLYID,CONTRACTID,CUSTNAME,CUSTNO,PAYMENT,BANKACCOUNT,BANK,BANKNO,ACCOUNTTYPE,ISDEFAULT,BANKTYPE,BANKBRANCH\
                        from tb_custaccount_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]用户账号表tb_custaccount_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 车辆状态tb_pawncarsituation_rpt
def trans_pawncarsituation():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]车辆状态tb_pawncarsituation_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '车辆状态tb_pawncarsituation_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '车辆状态tb_pawncarsituation_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]车辆状态tb_pawncarsituation_rpt->trytimes:', trytimes
        try:
            clear_table('tb_pawncarsituation_rpt_temp')
            sql = "INSERT INTO tb_pawncarsituation_rpt_temp \
                       (ID,APPLYID,CONTRACTNO,PAWNTYPE,PAWNSITUATION,GPSSTATUS,GPSINSTALL,SPAREKEYNUM,CARSTATUS,\
                        PROVINCE,CITY,CITYZONE,ADDRESSDETAIL,REMARK,CREATEBY,CREATETIME,HANDLEBY,CARSITUATION)\
                select ID,APPLYID,CONTRACTNO,PAWNTYPE,PAWNSITUATION,GPSSTATUS,GPSINSTALL,SPAREKEYNUM,CARSTATUS,\
                       PROVINCE,CITY,CITYZONE,ADDRESSDETAIL,REMARK,CREATEBY,CREATETIME,HANDLEBY,CARSITUATION\
                    from tio_xd_lb_pawncarsituation"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_pawncarsituation_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]车辆状态tb_pawncarsituation_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (
                end_time, end_time - start_time)
                return
            else:
                clear_table('tb_pawncarsituation_rpt')
                sql_db = "insert into tb_pawncarsituation_rpt  \
                             (ID,APPLYID,CONTRACTNO,PAWNTYPE,PAWNSITUATION,GPSSTATUS,GPSINSTALL,SPAREKEYNUM,CARSTATUS,\
                              PROVINCE,CITY,CITYZONE,ADDRESSDETAIL,REMARK,CREATEBY,CREATETIME,HANDLEBY,CARSITUATION) \
                      select ID,APPLYID,CONTRACTNO,PAWNTYPE,PAWNSITUATION,GPSSTATUS,GPSINSTALL,SPAREKEYNUM,CARSTATUS,\
                              PROVINCE,CITY,CITYZONE,ADDRESSDETAIL,REMARK,CREATEBY,CREATETIME,HANDLEBY,CARSITUATION\
                        from tb_pawncarsituation_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]车辆状态tb_pawncarsituation_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 贷款调查tb_investigatela_rpt
def trans_investigatela():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]贷款调查tb_investigatela_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '贷款调查tb_investigatela_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '贷款调查tb_investigatela_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]贷款调查tb_investigatela_rpt->trytimes:', trytimes
        try:
            clear_table('tb_investigatela_rpt_temp')
            sql = "INSERT INTO tb_investigatela_rpt_temp \
                       (ID,APPLYID,CUSTID,CUSTNAME,CUSTNO,INVESNAME,INVESID, ACCOMINVESNAME,REGDATE,SITENO,\
                       BUSANDMAP,RISKMEASURS,FAMILYOTHER,COMPANYOTHER,ADDREQUAL,COMPARSIONREG,HOUSETYPE,REMARK)\
                select ID,APPLYID,CUSTID,CUSTNAME,CUSTNO,INVESNAME,INVESID, ACCOMINVESNAME,REGDATE,SITENO,\
                       BUSANDMAP,RISKMEASURS,FAMILYOTHER,COMPANYOTHER,ADDREQUAL,COMPARSIONREG,HOUSETYPE,REMARK\
                  from tio_xd_lb_investigatela"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_investigatela_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]贷款调查tb_investigatela_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_investigatela_rpt')
                sql_db = "insert into tb_investigatela_rpt  \
                             (ID,APPLYID,CUSTID,CUSTNAME,CUSTNO,INVESNAME,INVESID, ACCOMINVESNAME,REGDATE,SITENO,\
                              BUSANDMAP,RISKMEASURS,FAMILYOTHER,COMPANYOTHER,ADDREQUAL,COMPARSIONREG,HOUSETYPE,REMARK) \
                      select ID,APPLYID,CUSTID,CUSTNAME,CUSTNO,INVESNAME,INVESID, ACCOMINVESNAME,REGDATE,SITENO,\
                              BUSANDMAP,RISKMEASURS,FAMILYOTHER,COMPANYOTHER,ADDREQUAL,COMPARSIONREG,HOUSETYPE,REMARK\
                        from tb_investigatela_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]贷款调查tb_investigatela_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 展期表tb_jextapplyinfo_rpt
def trans_jextapplyinfo():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]展期表tb_jextapplyinfo_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '展期表tb_jextapplyinfo_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '展期表tb_jextapplyinfo_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]展期表tb_jextapplyinfo_rpt->trytimes:', trytimes
        try:
            clear_table('tb_jextapplyinfo_rpt_temp')
            sql = "INSERT INTO tb_jextapplyinfo_rpt_temp \
                       (ID, LOANID, LOANNO, CUSTNAME, APPLYBY, APPLYDATE, AMT, EXHBEGIN, EXTSTARTDATE,\
                       EXTENDDATE, EXTCAPITALRATE, RECMONTHS, EXHAMT, EXHRATE, EXHCOST, EXHPAYTYPE, REMARK,\
                       CREATETIME, CREATEBY, MODIFYTIME, MODIFYBY, STATUS, SUBMITUSER, SUBMITDEPART, LEFTAMT,EXCONTRACTAMT)\
                select ID, LOANID, LOANNO, CUSTNAME, APPLYBY, APPLYDATE, AMT, EXHBEGIN, EXTSTARTDATE,EXTENDDATE, EXTCAPITALRATE,\
                       RECMONTHS,EXHAMT, EXHRATE, EXHCOST, EXHPAYTYPE, REMARK,DATE_FORMAT(CREATETIME, '%Y-%m-%d') as CREATETIME,\
                       CREATEBY,MODIFYTIME, MODIFYBY, STATUS, SUBMITUSER, SUBMITDEPART, LEFTAMT,EXCONTRACTAMT\
                 from tio_xd_lf_jextapplyinfo"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_jextapplyinfo_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]展期表tb_jextapplyinfo_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_jextapplyinfo_rpt')
                sql_db = "insert into tb_jextapplyinfo_rpt  \
                             (ID, LOANID, LOANNO, CUSTNAME, APPLYBY, APPLYDATE, AMT, EXHBEGIN, EXTSTARTDATE,\
                             EXTENDDATE, EXTCAPITALRATE, RECMONTHS, EXHAMT, EXHRATE, EXHCOST, EXHPAYTYPE, REMARK,\
                             CREATETIME, CREATEBY, MODIFYTIME, MODIFYBY, STATUS, SUBMITUSER, SUBMITDEPART, LEFTAMT,EXCONTRACTAMT) \
                      select ID, LOANID, LOANNO, CUSTNAME, APPLYBY, APPLYDATE, AMT, EXHBEGIN, EXTSTARTDATE,\
                             EXTENDDATE, EXTCAPITALRATE, RECMONTHS, EXHAMT, EXHRATE, EXHCOST, EXHPAYTYPE, REMARK,\
                             CREATETIME, CREATEBY, MODIFYTIME, MODIFYBY, STATUS, SUBMITUSER, SUBMITDEPART, LEFTAMT,EXCONTRACTAMT\
                        from tb_jextapplyinfo_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]展期表tb_jextapplyinfo_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 贷款终审表tb_applyaudit_last_rpt
def trans_applyaudit_last():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]贷款终审表tb_applyaudit_last_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '贷款终审表tb_applyaudit_last_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '贷款终审表tb_applyaudit_last_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]贷款终审表tb_applyaudit_last_rpt->trytimes:', trytimes
        try:
            clear_table('tb_applyaudit_last_rpt_temp')
            sql = "INSERT INTO tb_applyaudit_last_rpt_temp \
                       (APPLYID,LASTAUDITBY,LASTAUDITBYNAME,LASTAUDITTIME,LASTAUDITAMT,LASTAUDITRESULT,DENYBY,DENYBYNAME,\
                        DENYTIME,CANCELBY,CANCELBYNAME,CANCELTIME,INVALIDBY,INVALIDBYNAME,INVALIDTIME)\
                select APPLYID,LASTAUDITBY,LASTAUDITBYNAME,LASTAUDITTIME,LASTAUDITAMT,LASTAUDITRESULT,DENYBY,DENYBYNAME,\
                       DENYTIME,CANCELBY,CANCELBYNAME,CANCELTIME,INVALIDBY,INVALIDBYNAME,INVALIDTIME\
                  from tio_xd_lb_applyaudit_last"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select APPLYID from tb_applyaudit_last_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['APPLYID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]贷款终审表tb_applyaudit_last_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (
                end_time, end_time - start_time)
                return
            else:
                clear_table('tb_applyaudit_last_rpt')
                sql_db = "insert into tb_applyaudit_last_rpt  \
                             (APPLYID,LASTAUDITBY,LASTAUDITBYNAME,LASTAUDITTIME,LASTAUDITAMT,LASTAUDITRESULT,DENYBY,DENYBYNAME,\
                             DENYTIME,CANCELBY,CANCELBYNAME,CANCELTIME,INVALIDBY,INVALIDBYNAME,INVALIDTIME) \
                      select APPLYID,LASTAUDITBY,LASTAUDITBYNAME,LASTAUDITTIME,LASTAUDITAMT,LASTAUDITRESULT,DENYBY,DENYBYNAME,\
                             DENYTIME,CANCELBY,CANCELBYNAME,CANCELTIME,INVALIDBY,INVALIDBYNAME,INVALIDTIME\
                        from tb_applyaudit_last_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]贷款终审表tb_applyaudit_last_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 评估报告tb_evaluatereport_rpt
def trans_evaluatereport():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]评估报告tb_evaluatereport_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '评估报告tb_evaluatereport_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '评估报告tb_evaluatereport_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]评估报告tb_evaluatereport_rpt->trytimes:', trytimes
        try:
            clear_table('tb_evaluatereport_rpt_temp')
            sql = "INSERT INTO tb_evaluatereport_rpt_temp \
                       (ID,APPLYID,CUSTNAME,CARDTYPE,CARDNO,MOBILE,EVALUATEPRICE,EVALUATEDATE,EVALUATEMAN,SITENO,CARNO,BRAND,BUYDATE,BUYPRICE,BUYMODE,MONTHPAY,\
                        DRIVINGDISTANCE,ENGINENO,CARTYPE,DRIVENO,SETTLEMENT,SETTLEMENTAMT,qzbxd,csx,dqx,dszzrx,bjmp,otherc,scuttle,seat,navigate,otherconf,\
                        OTHERCONFIGURE,registercard,drivercard,receipt,carboatbill,yearbill,TOOLS,JACK,TIRE,STARTMOTOR,STARTMOTORREASON,ENGINEFLAG,ENGINEREASON,\
                        SPEEDCHANGER,SPEEDCHANGERREASON,ELECTRICAL,ELECTRICALREASON,HOUSEADDR,HOUSENO,HOUSEAREA,USEAREA,YEAR,PLANUSE,GETMETHOD,OWNTYPE,COMMUNITY,\
                        PERPSONS,ONLINECOMMENT,ACTUALINSPECT,EVALUATETYPE,STATUS)\
                select ID,APPLYID,CUSTNAME,CARDTYPE,CARDNO,MOBILE,EVALUATEPRICE,EVALUATEDATE,EVALUATEMAN,SITENO,CARNO,BRAND,BUYDATE,BUYPRICE,BUYMODE,MONTHPAY,\
                        DRIVINGDISTANCE,ENGINENO,CARTYPE,DRIVENO,SETTLEMENT,SETTLEMENTAMT,qzbxd,csx,dqx,dszzrx,bjmp,otherc,scuttle,seat,navigate,otherconf,\
                        OTHERCONFIGURE,registercard,drivercard,receipt,carboatbill,yearbill,TOOLS,JACK,TIRE,STARTMOTOR,STARTMOTORREASON,ENGINEFLAG,ENGINEREASON,\
                        SPEEDCHANGER,SPEEDCHANGERREASON,ELECTRICAL,ELECTRICALREASON,HOUSEADDR,HOUSENO,HOUSEAREA,USEAREA,YEAR,PLANUSE,GETMETHOD,OWNTYPE,COMMUNITY,\
                        PERPSONS,ONLINECOMMENT,ACTUALINSPECT,EVALUATETYPE,STATUS\
                   from tio_xd_lb_evaluatereport"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_evaluatereport_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]评估报告tb_evaluatereport_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_evaluatereport_rpt')
                sql_db = "insert into tb_evaluatereport_rpt  \
                             (ID,APPLYID,CUSTNAME,CARDTYPE,CARDNO,MOBILE,EVALUATEPRICE,EVALUATEDATE,EVALUATEMAN,SITENO,CARNO,BRAND,BUYDATE,BUYPRICE,BUYMODE,MONTHPAY,\
                              DRIVINGDISTANCE,ENGINENO,CARTYPE,DRIVENO,SETTLEMENT,SETTLEMENTAMT,qzbxd,csx,dqx,dszzrx,bjmp,otherc,scuttle,seat,navigate,otherconf,\
                              OTHERCONFIGURE,registercard,drivercard,receipt,carboatbill,yearbill,TOOLS,JACK,TIRE,STARTMOTOR,STARTMOTORREASON,ENGINEFLAG,ENGINEREASON,\
                              SPEEDCHANGER,SPEEDCHANGERREASON,ELECTRICAL,ELECTRICALREASON,HOUSEADDR,HOUSENO,HOUSEAREA,USEAREA,YEAR,PLANUSE,GETMETHOD,OWNTYPE,COMMUNITY,\
                              PERPSONS,ONLINECOMMENT,ACTUALINSPECT,EVALUATETYPE,STATUS) \
                      select ID,APPLYID,CUSTNAME,CARDTYPE,CARDNO,MOBILE,EVALUATEPRICE,EVALUATEDATE,EVALUATEMAN,SITENO,CARNO,BRAND,BUYDATE,BUYPRICE,BUYMODE,MONTHPAY,\
                              DRIVINGDISTANCE,ENGINENO,CARTYPE,DRIVENO,SETTLEMENT,SETTLEMENTAMT,qzbxd,csx,dqx,dszzrx,bjmp,otherc,scuttle,seat,navigate,otherconf,\
                              OTHERCONFIGURE,registercard,drivercard,receipt,carboatbill,yearbill,TOOLS,JACK,TIRE,STARTMOTOR,STARTMOTORREASON,ENGINEFLAG,ENGINEREASON,\
                              SPEEDCHANGER,SPEEDCHANGERREASON,ELECTRICAL,ELECTRICALREASON,HOUSEADDR,HOUSENO,HOUSEAREA,USEAREA,YEAR,PLANUSE,GETMETHOD,OWNTYPE,COMMUNITY,\
                              PERPSONS,ONLINECOMMENT,ACTUALINSPECT,EVALUATETYPE,STATUS\
                        from tb_evaluatereport_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]评估报告tb_evaluatereport_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 催收表tb_urge_rpt
def trans_urge():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]催收表tb_urge_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '催收表tb_urge_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '催收表tb_urge_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]催收表tb_urge_rpt->trytimes:', trytimes
        try:
            clear_table('tb_urge_rpt_temp')
            sql = "INSERT INTO tb_urge_rpt_temp \
                       (ID,PAYMENTID,LINKMAN,URGEMODE,CONTENT,SPENDTIME,URGEDATE,URGEMAN,STATUS,CREATEBY,\
                        CREATETIME,PROMISEPAY,PROMISEPAYDATE,URGEPHASES,SITUATION,CUSTSTATUS,INSTATUS)\
                SELECT urge.ID, urge.PAYMENTID, urge.LINKMAN, urge.URGEMODE, urge.CONTENT, urge.SPENDTIME, urge.URGEDATE, d.USERNAME AS URGEMAN,\
                       urge.STATUS, urge.CREATEBY, urge.CREATETIME, urge.PROMISEPAY, urge.PROMISEPAYDATE, urge.URGEPHASES, urge.SITUATION, \
                       urge.CUSTSTATUS, urge.INSTATUS \
                  FROM tio_xd_la_urge urge \
                  LEFT JOIN tio_xd_la_dondealing d ON urge.PAYMENTID = d.LOANID"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_urge_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]催收表tb_urge_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_urge_rpt')
                sql_db = "insert into tb_urge_rpt  \
                             (ID,PAYMENTID,LINKMAN,URGEMODE,CONTENT,SPENDTIME,URGEDATE,URGEMAN,STATUS,CREATEBY,\
                              CREATETIME,PROMISEPAY,PROMISEPAYDATE,URGEPHASES,SITUATION,CUSTSTATUS,INSTATUS) \
                      select ID,PAYMENTID,LINKMAN,URGEMODE,CONTENT,SPENDTIME,URGEDATE,URGEMAN,STATUS,CREATEBY,\
                             CREATETIME,PROMISEPAY,PROMISEPAYDATE,URGEPHASES,SITUATION,CUSTSTATUS,INSTATUS\
                        from tb_urge_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]催收表tb_urge_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 放款条件表tb_loancond_rpt
def trans_loancond():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]放款条件表tb_loancond_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '放款条件表tb_loancond_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '放款条件表tb_loancond_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]放款条件表tb_loancond_rpt->trytimes:', trytimes
        try:
            clear_table('tb_loancond_rpt_temp')
            sql = "INSERT INTO tb_loancond_rpt_temp \
                       (ID, APPLYID, INFOTYPE, CHECKFLAG, REMARK)\
                select ID, APPLYID, INFOTYPE, CHECKFLAG, REMARK  from tio_xd_lb_loancond"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_loancond_rpt_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]放款条件表tb_loancond_rpt_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_loancond_rpt')
                sql_db = "insert into tb_loancond_rpt  \
                             (ID, APPLYID, INFOTYPE, CHECKFLAG, REMARK) \
                      select ID, APPLYID, INFOTYPE, CHECKFLAG, REMARK\
                        from tb_loancond_rpt_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]放款条件表tb_loancond_rpt结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


# 档案号tb_lm_file_no
def trans_file_no():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]档案号tb_lm_file_no开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '档案号tb_lm_file_no重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '档案号tb_lm_file_no重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]档案号tb_lm_file_no->trytimes:', trytimes
        try:
            clear_table('tb_lm_file_no_temp')
            sql = "INSERT INTO tb_lm_file_no_temp \
                       (id,paymentid,apply_id,file_no,create_date,TEMPLATE_ID)\
                SELECT id,paymentid,apply_id,file_no,create_date,TEMPLATE_ID FROM tio_xd_lm_file_no"
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tb_lm_file_no_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]档案号tb_lm_file_no_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tb_lm_file_no')
                sql_db = "insert into tb_lm_file_no  \
                             (id,paymentid,apply_id,file_no,create_date,TEMPLATE_ID) \
                      select id,paymentid,apply_id,file_no,create_date,TEMPLATE_ID\
                        from tb_lm_file_no_temp"
                r = rpt_db.query(sql_db)
                rpt_db.commit()
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]档案号tb_lm_file_no结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True


def trans_workdate():
    reload(sys)
    sys.setdefaultencoding('utf8')
    # 当前工作日
    cur_workdate = 0
    # 本月工作日
    total_workdate = 0
    # 工作日标识：0否 1是
    isWorkDate = 0
    first_date = datetime.date.today().replace(day=1)
    first_date2 = first_date
    next_month = (first_date + datetime.timedelta(days=31)).replace(day=1)
    # first_traverse = 1
    workdates = []
    # 建立数据库连接
    try:
        rpt_db = connect_with(db_rpt_conf)
        print('[基础数据转化]刷新工作日配置表 tpm_workdate_config 数据开始 %s' % datetime.datetime.now())
        sql = "select DAY,ISWORKDAY from tb_holidaysset_rpt r where r.DAY>='%s' and r.DAY<'%s'" % (
            first_date, next_month)
        datas = rpt_db.query_data(sql=sql)
        # 以结果集新建字典{DAY:ISWORKDAY}
        holidayset = dict([x.values() for x in datas])
        # 删除重复数据
        sql = "delete from tpm_workdate_config where statdate >='%s' and statdate<'%s'" \
              % (first_date2.strftime('%Y%m%d'), next_month.strftime('%Y%m%d'))
        r = rpt_db.query(sql)
        rpt_db.commit()

        while first_date < next_month:
            if first_date.strftime('%Y-%m-%d') in holidayset.keys():
                # 当天为工作日
                if holidayset[first_date.strftime('%Y-%m-%d')] == 1:
                    cur_workdate += 1
                    total_workdate += 1
                    isWorkDate = 1
                else:
                    isWorkDate = 0
            else:
                # 当天为周末
                if first_date.weekday() >= 5:
                    isWorkDate = 0
                else:
                    cur_workdate += 1
                    total_workdate += 1
                    isWorkDate = 1
            # 初始化第一次遍历的变量
            # if first_traverse:
            #     cur_workdate = 1
            #     total_workdate = 1
            #     first_traverse = 0
            # 插入新数据
            sql = """insert into tpm_workdate_config(statdate, curWorkDate, isWorkDate)
                      values(%s,%s,%s)""" % (first_date.strftime('%Y%m%d'), cur_workdate, isWorkDate)
            r = rpt_db.query(sql)
            rpt_db.commit()

            first_date = first_date + datetime.timedelta(days=1)

        # 更新当月工作日天数
        sql = """update tpm_workdate_config set workDate = %s where statdate >='%s' and statdate<'%s'""" \
              % (total_workdate, first_date2.strftime('%Y%m%d'), next_month.strftime('%Y%m%d'))
        r = rpt_db.query(sql)
        rpt_db.commit()

        print('[基础数据转化]刷新工作日配置表 tpm_workdate_config 数据结束 %s' % datetime.datetime.now())
    except Exception, ex:
        traceback.print_exc()
    finally:
        rpt_db.close()
    return True


# 经纪人信息表转化
# 20190523 按照邮件[修改Bi系统经纪人业绩明细表格职位的展示]要求修改经纪人的职位为最新职位
def trans_agent_info():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]经纪人信息表tb_emp_rpt开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print 'tb_emp_rpt重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print 'tb_emp_rpt重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]员工信息表tb_emp_rpt->trytimes:', trytimes
        try:
            clear_table('tpm_agent_info_temp')
            sql = """INSERT INTO tpm_agent_info_temp(id,agentCode,created_time,updated_time,id_type,id_code,real_name,sex,nation,birth,
                    valid_date_start,valid_date_end,address,authority,approval_status,no_pass_reason,approval_time_start,
                    approval_time_end,inviterDuties,inviterID,inviterCode,inviterName,sitecode,orgid,orgname,orgleaderid,orgleader,
                    inviter_empid,inviter_empcode,inviter_empname,inviter_empduty)
                SELECT a.id,/*CONCAT('MID',LPAD(a.id,8,0))*/ f.broker_no agentCode,a.created_time,a.updated_time,a.id_type,a.id_code,a.real_name,a.sex,a.nation,a.birth,
                    a.valid_date_start,a.valid_date_end,a.address,a.authority,a.approval_status,a.no_pass_reason,a.approval_time_start,
                    a.approval_time_end,max(d.`DUTIES`) inviterDuties,d.id inviterID,ifnull(d.`EMPNO`,c.group_owner) inviterCode,d.`EMPNAME` inviterName,e.sitecode,
                    e.ID,e.orgname,e.orgleaderid,e.orgleader,d.id inviter_empid,d.EMPNO inviter_empcode,d.EMPNAME inviter_empname,max(d.`DUTIES`) inviter_empduty
                  FROM tio_xd_fdb_real_name a
                  left join tio_xd_fdb_login_user f on a.id = f.real_name_id
                  LEFT JOIN `tio_xd_fdb_broker_relation` b ON f.id = b.`login_user_id`
                  LEFT JOIN `tio_xd_fdb_broker_group` c ON b.`broker_group_id` = c.id
                  -- LEFT JOIN tio_xd_sys_emp_his d ON c.`group_owner` = d.`EMPNO` and a.approval_time_end is not null and a.approval_time_end>=d.starttime and a.approval_time_end<d.endtime
                  LEFT JOIN tio_xd_sys_emp d ON c.`group_owner` = d.`EMPNO`
                  left join tio_xd_sys_org e on d.orgid = e.id group by a.id"""
            rpt_db.query(sql)
            rpt_db.commit()

            # 更新一级经纪人邀请的二级经纪人信息
            sql = """
              update tpm_agent_info_temp t1,
                     tpm_agent_info_temp t2
                 set t1.inviterDuties='经纪人',
                     t1.inviterID=t2.id,
                     t1.inviterName=t2.real_name,
                     t1.sitecode=t2.sitecode,
                     t1.orgid=t2.orgid,
                     t1.orgname=t2.orgname,
                     t1.orgleaderid=t2.orgleaderid,
                     t1.orgleader=t2.orgleader,
                     t1.inviter_empid=t2.inviterID,
                     t1.inviter_empcode=t2.inviterCode,
                     t1.inviter_empname=t2.inviterName,
                     t1.inviter_empduty=t2.inviter_empduty
               where t1.inviterCode = t2.agentCode;
            """
            rpt_db.query(sql)
            rpt_db.commit()

            # 更新经纪人90天以内离职的客户经理工号
            sql = """UPDATE tpm_agent_info_temp a ,tio_hr_sys_emp b
                      SET a.`empLeaveCode` = b.`workCode`
                    WHERE a.`id_code` = b.`idcard`
                      AND b.`statDate` = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY),'%Y%m')
                      AND b.`positionName` = '客户经理'
                      AND TIMESTAMPDIFF(DAY,b.leaveDate,date(a.`approval_time_end`))<=90
                      AND b.`workCode` LIKE 'WJ%'"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            #　经纪人转客户经理信息更新
            sql = """update tpm_agent_info_temp aaa,
                         (SELECT aa.`IDCARD`,aa.empno,aa.`JOBDATE`
                            FROM tb_emp_rpt aa,
                                 (SELECT b.IDCARD,MIN(B.JOBDATE)JOBDATE
                                              FROM tpm_agent_info a ,tb_emp_rpt b
                                             WHERE a.id_code = b.IDCARD
                                                 AND b.EMPNO LIKE 'WJ%'
                                                 AND B.JOBDATE>='2019-01-01'
                                                 AND B.JOBDATE>A.approval_time_end
                                             GROUP BY b.IDCARD) bb
                           WHERE aa.`IDCARD` = bb.idcard
                             AND aa.`JOBDATE` = bb.jobdate
                             AND aa.`JOBDATE`>='2019-01-01'
                           group by aa.`IDCARD`)bbb
                     set aaa.empjobcode = bbb.empno,aaa.empjobdate = bbb.JOBDATE
                   where aaa.id_code = bbb.IDCARD"""
            rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select id from tpm_agent_info_temp"
            result_r = rpt_db.querySql(sql, field=['id'])

            if len(result_r) == 0:
                end_time = datetime.datetime.today()
                print "[基础数据转化]员工信息表tpm_agent_info_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tpm_agent_info')
                sql_db = """
                  insert into tpm_agent_info
                        (id,agentCode,created_time,updated_time,id_type,id_code,real_name,sex,nation,birth,valid_date_start,valid_date_end,address,authority,approval_status,no_pass_reason,approval_time_start,
                         approval_time_end,inviterDuties,inviterID,inviterCode,inviterName,empLeaveCode,sitecode,orgid,orgname,orgleaderid,orgleader,empJobCode,empJobDate,
                         inviter_empid,inviter_empcode,inviter_empname,inviter_empduty)
                  select id,agentCode,created_time,updated_time,id_type,id_code,real_name,sex,nation,birth,valid_date_start,valid_date_end,address,authority,approval_status,no_pass_reason,approval_time_start,
                         approval_time_end,inviterDuties,inviterID,inviterCode,inviterName,empLeaveCode,sitecode,orgid,orgname,orgleaderid,orgleader,empJobCode,empJobDate,
                         inviter_empid,inviter_empcode,inviter_empname,inviter_empduty
                    from tpm_agent_info_temp"""
                rpt_db.query(sql_db)
                rpt_db.commit()

        except Exception:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]经纪人信息表tpm_agent_info结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break
    rpt_db.close()
    return True

# 车贷信贷订单与投哪标的关系表 tbl_tn_order_detail
def trans_tn_order_detail():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)  # 连接数据库

    start_time = datetime.datetime.today()
    print "[基础数据转化]车贷信贷订单与投哪标的关系表 tbl_tn_order_detail开始时间: %s" % (start_time)
    trytimes = 0
    while trytimes <= 3:
        print '车贷信贷订单与投哪标的关系表 tbl_tn_order_detail重新获取连接:trytimes=', trytimes, datetime.datetime.today()
        inserted_num = 0
        rpt_db.close()  # 关闭数据库连接(防止之前有连接)
        rpt_db = connect_with(db_rpt_conf)  # 连接数据库
        print '车贷信贷订单与投哪标的关系表 tbl_tn_order_detail重新获取连接完成:trytimes=', trytimes, datetime.datetime.today()
        trytimes += 1
        print '[基础数据转化]车贷信贷订单与投哪标的关系表 tbl_tn_order_detail->trytimes:', trytimes
        try:
            clear_table('tbl_tn_order_detail_temp')
            sql = """INSERT INTO tbl_tn_order_detail_temp(applyid,borrowID,productType,tn_applyid,tn_contractNo,paymentid,exhiType)
                  SELECT aa.applyid,aa.borrowID,aa.productType,aa.tn_applyid,aa.tn_contractNo,aa.paymentid,aa.exhiType
                    FROM (
                            SELECT b.APPLYID,c.id borrowID,d.PRODUCTTYPE productType,a.applyid tn_applyid,a.contract_no tn_contractNo,
                                   a.paymentid,IFNULL(a.exhi_type,0)exhiType
                              FROM tio_tn_tn_lend a ,tio_xd_lm_payment b,tio_tn_dw_borrow c,tb_applyinfo_rpt d
                             WHERE a.paymentid = b.id
                               AND a.borrowid = c.id
                               AND b.APPLYID = d.APPLYID
                               AND c.`status` IN(3,73)
                               and a.paymentid <>''
                               and a.paymentid is not null
                               AND IFNULL(a.from_source,'投哪-小额贷款') <>'投哪-汽车融资租赁'
                             UNION ALL
                            SELECT b.APPLYID,c.id borrowID,d.PRODUCTTYPE productType,a.applyid tn_applyid,a.contract_no tn_contractNo,
                                   a.paymentid,IFNULL(a.exhi_type,0)exhiType
                              FROM tio_tn_tn_lend a ,tio_xd_lm_payment b,tio_tn_dw_borrow c,tb_applyinfo_rpt d
                             WHERE a.applyid = cast(b.applyid as char)
                               AND a.borrowid = c.id
                               AND b.APPLYID = d.APPLYID
                               AND c.`status` IN(3,73)
                               and a.paymentid <>''
                               and a.paymentid is not null
                               AND IFNULL(a.from_source,'投哪-小额贷款') <>'投哪-汽车融资租赁'
                             UNION ALL
                            SELECT b.applyid,a.borrow_id borrowID,b.PRODUCTTYPE productType,a.apply_id tn_applyid,a.contract_no tn_contractNo,
                                   cast(a.id as char),0 exhiType
                              FROM tio_tn_fms_loan a ,tb_applyinfo_rpt b
                             WHERE a.borrow_id IS NOT NULL
                               AND a.order_id = b.applyid
                           )aa
                  ORDER BY aa.applyid,aa.tn_applyid"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            sql = """UPDATE tbl_tn_order_detail_temp a ,tb_contract_rpt b
                     SET a.`contractNO` = b.`CONTRACTNO`
                   WHERE a.`applyID` = b.applyid"""
            r = rpt_db.query(sql)
            rpt_db.commit()

            # 判断是否有数据
            sql = "select ID from tbl_tn_order_detail_temp"
            result_r = rpt_db.querySql(sql, field=['ID'])

            if len(result_r) <= 10000:
                end_time = datetime.datetime.today()
                print "[基础数据转化]车贷信贷订单与投哪标的关系表 tbl_tn_order_detail_temp数据异常! 结束时间：%s，运行时间：%s" % (end_time, end_time - start_time)
                return
            else:
                clear_table('tbl_tn_order_detail')
                sql = """INSERT INTO tbl_tn_order_detail(applyid,borrowID,productType,tn_applyid,tn_contractNo,paymentid,exhiType,contractNO)
                      select applyid,borrowID,productType,tn_applyid,tn_contractNo,paymentid,exhiType,contractNO
                        from tbl_tn_order_detail_temp"""
                r = rpt_db.query(sql)
                rpt_db.commit()

        except Exception, ex:  # 插入数据错误
            traceback.print_exc()
            raise
        else:
            end_time = datetime.datetime.today()
            print "[基础数据转化]车贷信贷订单与投哪标的关系表 tbl_tn_order_detail结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
            break

    rpt_db.close()
    return True


def trans_agent_level():
    reload(sys)
    sys.setdefaultencoding('utf8')
    rpt_db = connect_with(db_rpt_conf)
    base_db = connect_with(db_conf)
    statdate = datetime.date.today()
    start_time = datetime.datetime.today()
    print "[基础数据转化]经纪人等级数据同步 tbl_agent_level 开始时间: %s" % (start_time)
    try:
        sql = 'delete from tbl_agent_level where statDate = {statdate}'.format(statdate=statdate.strftime('%Y%m'))
        rpt_db.updateBysql(sql)
        sql = """
          select '{statdate}', a.broker_no, a.rank_title, a.rank_level,
                 concat(case a.rank_title
                          when 'YYD' then '有一单'
                          when 'BRONZE' then '青铜'
                          when 'SILVER' then '白银' end,
                        case a.rank_level
                          when '1' then 'LV1'
                          when '2' then 'LV2'
                          else '' end) as rank_name
            from tb_fdb_login_user a;
        """.format(statdate=statdate.strftime('%Y%m'))
        data = base_db.querySql(sql, field=['statDate', 'broker_no', 'rank_title', 'rank_level', 'rank_name'])
        rpt_db.batchInsert('tbl_agent_level', data)
        end_time = datetime.datetime.today()
        print "[基础数据转化]经纪人等级数据同步 tbl_agent_level 结束时间: %s,运行时间: %s" % (end_time, end_time - start_time)
    except Exception:
        traceback.print_exc()
        raise
    finally:
        rpt_db.close()
        base_db.close()


if __name__ == '__main__':
    exe(eval(sys.argv[1]), sys.argv[2])
    # trans_workdate()
    # trans_emp()
    # trans_emp_his()
    # trans_org()
    # trans_org_his()
    # trans_bigarea()
    # trans_area()
    # trans_provincemanage()
    # trans_site()
    # trans_site_his()
    # trans_applyinfo()
    # trans_contract()
    # trans_payment()
    # trans_return()
    # trans_gpsinstall()
    # trans_sitetarget()
    # trans_dist()
    # trans_otherfee()
    # trans_fee()
    # trans_applyflow()
    # trans_applyaudit()
    # trans_applyaudit_last()
    # trans_unified_dependent()
    # trans_costreduction_detail()
    # trans_custinfo()
    # trans_custaccount()
    # trans_pawncarsituation()
    # trans_investigatela()
    # trans_jextapplyinfo()
    # trans_evaluatereport()
    # trans_urge()
    # trans_loancond()
    # trans_file_no()
    # trans_agent_info()
    # trans_tn_order_detail()
    # trans_workdate()
    # trans_agent_level()