#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   fengbo
# **  创建日期: yyyy/mm/dd
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **

import sys,os
import datetime
import time
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *
from hqltools import *
#from sqltools import *

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[1]
province = sys.argv[2]
provider = sys.argv[3]
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
#===========================================================================================
#自定义变量声明---源表声明
#===========================================================================================
    source_tb = "dmp_ci_user_m_bh"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_um_m_ht"
    target_tmp = "dmp_um_m_ht_tmp"
    target_tb_np = "dmp_um_m_ht_tmp_np"
    group_day_tb = "dmp_um_m_ht_day"
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_m_uid   string,
            m_id    string,
            id2uid_score    double,
            uid2id_score    double
        )
        partitioned by (provider string,province string,day_id string,hour_id string,mflag string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())

    hivesql.append('''
        create table if not exists %(target_tb_np)s
        (
            mix_m_uid   string,
            m_id    string,
            id2uid_score    double,
            uid2id_score    double,
            mflag string
        )
        partitioned by (provider string,province string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb_np)s';
    ''' % vars())

    hivesql.append('''
        create table if not exists %(target_tmp)s
        (
            mix_m_uid   string,
            score    double,
            mflag string
        )
        partitioned by (provider string,province string)
        row format delimited
        fields terminated by '\\t'
        COLLECTION ITEMS TERMINATED BY ','
        location '%(HIVE_TB_HOME)s/%(target_tmp)s';
    ''' % vars())

    hivesql.append('''
        create table if not exists %(group_day_tb)s
        (
            s_phone_no string,
            m_id string
        )
        partitioned by (provider string,province string,day_id string,mflag string)
        row format delimited
        fields terminated by '\\t'
        COLLECTION ITEMS TERMINATED BY ','
        location '%(HIVE_TB_HOME)s/%(group_day_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
#===========================================================================================
#程序执行
#===========================================================================================
    back_day = get_date_of_back_someday(ARG_OPTIME, 30)
    hour_id = ARG_OPTIME+"00"
    hivesql = []
    hivesql.append('''
        insert overwrite table %(target_tmp)s
        partition (provider = '%(provider)s',province = '%(province)s')
        select s_phone_no,1/count(distinct m_id) as score ,mflag from %(source_tb)s where day_id>=%(back_day)s
        and day_id <= %(ARG_OPTIME)s and provider = '%(provider)s' and province = '%(province)s'
        group by s_phone_no,mflag
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql=[]
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(group_day_tb)s
        partition (provider,province,day_id,mflag)
        select s_phone_no,m_id,provider,province,day_id,mflag from %(source_tb)s where day_id=%(ARG_OPTIME)s
        and provider = '%(provider)s' and province = '%(province)s'
        group by s_phone_no,m_id,province,provider,day_id,mflag
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql=[]
    hivesql.append('''
        set mapreduce.map.failures.maxpercent=5;
        add file %(current_path)s/cal_uidid_score.py;
        insert overwrite table %(target_tb_np)s
        partition (provider = '%(provider)s',province = '%(province)s')
        select mix_m_uid,m_id,b.score,a.score,a.mflag from (
            select transform (m_id,collect_set(s_phone_no),mflag)
            using 'python cal_uidid_score.py'
            as (m_id,phone_no,score,mflag)
            from %(group_day_tb)s where day_id>=%(back_day)s and day_id <= %(ARG_OPTIME)s
            and provider = '%(provider)s' and province = '%(province)s'
            and m_id != ''
            group by m_id,mflag
        )a join %(target_tmp)s b on a.phone_no = b.mix_m_uid and a.mflag=b.mflag
        group by mix_m_uid,m_id,a.score,b.score,a.mflag
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',day_id=%(ARG_OPTIME)s,hour_id=%(hour_id)s,mflag)
    select mix_m_uid,m_id,id2uid_score,uid2id_score,mflag from %(target_tb_np)s where
    provider = '%(provider)s' and province = '%(province)s'
    ''' % vars())
    HiveExe(hivesql, name, dates)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
