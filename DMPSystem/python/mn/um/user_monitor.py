#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   xxx
# **  创建日期: yyyy/mm/dd
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
import os,sys
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

#引入自定义包
from settings import *
from hqltools import *

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
    source_all_tb = "dmp_um_m_ht"
    #===========================================================================================
    #自定义变量声明---目标表声明
    target_tb = "dmp_mn_kpi_bd"
    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            kpi string,
            value double
        )
        partitioned by (provider string,province string,net_type string,day_id string,module string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s'
    ''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #插入目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s partition (provider,province,net_type,day_id,module)
        select concat_ws('|','um','um_1','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day'),
               count(distinct s_phone_no),provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'um' as module
        from %(source_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s' group by province,provider,day_id;
    ''' % vars())

    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert into table %(target_tb)s partition (provider,province,net_type,day_id,module)
        select concat_ws('|','um','um_2','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day'),
               count(distinct mix_m_uid),provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'um' as module
        from %(source_all_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s' group by province,provider,day_id;
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    flag_arr = {'imei':'um_rel_2','imsi':'um_rel_3','mac':'um_rel_4','idfa':'um_rel_5','android_id':'um_rel_6'}

    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s partition (provider,province,net_type,day_id,module)
        select concat_ws('|','um_rel',flag_name,'','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day'),
            num,provider,province,net_type,day_id,module from (
            select case when mflag='phone_no' then 'um_rel_1'
                        when mflag='imei' then 'um_rel_2'
                        when mflag='imsi' then 'um_rel_3'
                        when mflag='mac' then 'um_rel_4'
                        when mflag = 'idfa' then 'um_rel_5'
                        when mflag = 'android_id' then 'um_rel_6'
                        else '' end as flag_name,
            count(distinct m_id) as num,provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'um_rel' as module
            from %(source_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            and mflag in ('phone_no','imei','imsi','mac','idfa','android_id')
            group by province,provider,day_id,mflag
        ) bb;
    ''' % vars())
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)

