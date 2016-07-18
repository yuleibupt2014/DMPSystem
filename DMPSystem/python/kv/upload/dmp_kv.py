#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# 调用示例：dmp_upload_inter_tab_bh.py {province} {tablename} {yyyyMMddHH} {yyyyMMddHH}
#    小时级正常流程：xxx.py beijing dmp_uc_tags_bh 2014110110 2014110110
#    紧急流程： xxx.py beijing dmp_uc_tags_bh 2014110100 2014112523
# ** ---------------------------------------------------------------------------------------
# **
import os, sys
import re
import string
import types
import ConfigParser
import time
import datetime

# PYTHONPATH
current_path = os.path.dirname(os.path.abspath(__file__))
mix_home = current_path + '/../../../'
python_path = []
python_path.append(mix_home + '/conf')
sys.path[:0] = python_path

from settings import *
from hqltools import *

name = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:].rstrip('.py')
province = sys.argv[1]  # 省份
tablename = sys.argv[2]  # 表名
startTime = sys.argv[3]  # 开始时间
endTime = sys.argv[4]  # 结束时间
            
# 为了多个表能够同时上传
name = name + "_" + tablename

# 校验格式并构建查询条件
cond = ''
if(len(startTime) == 10 and len(endTime) == 10):
    target_tablename = "dmp_upload_inter_tab_bh"
    cond = 'day_id >= ' + startTime[0:8] + ' and day_id <= ' + endTime[0:8] 
    cond += ' and hour_id >= ' + startTime + ' and hour_id <= ' + endTime   
elif(len(startTime) == 8 and len(endTime) == 8):
    target_tablename = "dmp_upload_inter_tab_bd"
    cond = 'day_id >= ' + startTime[0:8] + ' and day_id <= ' + endTime[0:8]
else:
    tmpLog = '时间格式不对,必须为：yyyyMMdd[hh]! 退出执行！'
    print tmpLog
    sys.exit()

try:
    dicts = {}
    Pama(dicts, startTime)
    Start(name, startTime)
#===========================================================================================
# 自定义变量声明---目标表声明
#===========================================================================================

    #加载配置
    conf = ConfigParser.ConfigParser()
    conf.read(current_path+"/kv.cfg")
    theme = conf.get(tablename, "theme")
    userid = conf.get(tablename,"userid")
    type = conf.get(tablename,"type")
    if(type=='none'):
        type=""
        c_type=""
        cond_type = ""
    else:
        type_value = conf.get(tablename, "type_value")
        if(type_value == 'none'):
            cond_type = ""
        else:
            cond_type = " and "+type+"='"+type_value+"'"
        type=conf.get(tablename,"type")+" as type,"
        c_type="type,"
    value = conf.get(tablename,"value")
    datatime = conf.get(tablename,"datatime")
    
    if(datatime == 'hour_id'):
        partition = "day_id string,hour_id string"
        partition_str = "day_id,hour_id"
    elif(datatime == 'day_id'):
        partition = "day_id string"
        partition_str = "day_id"
    else:
        partition = ""
        partition_str = ""
    
    # 分隔符加载
    sep1 = conf.get("separator","sep1");
    sep2 = conf.get("separator","sep2");
    
    
    pri = conf.get("province",province)+"_"+theme
    
    hivesql = []
    hivesql.append(''' 
        create table if not exists %(HIVE_DATABASE)s.%(target_tablename)s 
        ( key string, value string) 
        partitioned by (%(partition)s,tab string,province string) 
        row format delimited fields terminated by '\t' 
        stored as textfile 
        LOCATION '%(HIVE_TB_HOME)s/%(target_tablename)s'
    ''' % vars())
    HiveExe(hivesql, name, startTime)
    
    # 将不同行的数据拼接并插入接口表
    hivesql = []
    hivesql.append('add file ' + current_path + "/mapper.py")
    hivesql.append('add file ' + current_path + "/reducer.py")
    hivesql.append('add file ' + current_path + "/kv.cfg")
    hivesql.append("add jar %(JAR_MIX)s " % vars())
    hivesql.append("create temporary function UDFConcat_ws as 'com.ailk.hdm.hive.udf.upload.UDFConcat_ws'")
    hivesql.append("create temporary function UdfValueEncrypt as 'com.ailk.hdm.hive.udf.UdfValueEncrypt'")
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        from (
            from (
                select '%(tablename)s' as tablename,%(userid)s as userid,%(type)s 
                     UDFConcat_ws('%(sep2)s',%(value)s) as value,
                     %(partition_str)s 
                from %(HIVE_DATABASE)s.%(tablename)s 
                where province='%(province)s' and %(cond)s %(cond_type)s  
                cluster by %(partition_str)s,userid
            ) a 

	    map a.tablename,a.userid,%(c_type)s a.value,%(datatime)s
                using 'python mapper.py' as value,%(datatime)s
	    cluster by %(datatime)s 
        ) b
                
        insert overwrite table %(HIVE_DATABASE)s.%(target_tablename)s 
            partition (%(partition_str)s,tab,province)
            reduce '%(pri)s',b.value,%(datatime)s,'%(tablename)s','%(province)s'
            using 'python reducer.py' as key,value,%(partition_str)s,tablename,province
    ''' % vars())
    HiveExe(hivesql, name, startTime)
    
        
    # 计算每个分区的数量并插入该表
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(HIVE_DATABASE)s.%(target_tablename)s 
        partition (%(partition_str)s,tab,province) 
        select key,value,%(partition_str)s,'%(tablename)s_num' as tab,'%(province)s'
        from(
            select split(key,'-')[0] as key,count(1) as value,%(partition_str)s
                from %(HIVE_DATABASE)s.%(target_tablename)s 
                where province='%(province)s' and %(cond)s 
                and tab='%(tablename)s' 
                group by split(key,'-')[0],%(partition_str)s
        )t
    ''' % vars())
    HiveExe(hivesql, name, startTime)
    
#===========================================================================================
# 程序结束
    End(name, startTime)
# 异常处理
except Exception, e:
    Except(name, startTime, e)
