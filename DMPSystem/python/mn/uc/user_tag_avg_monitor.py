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
    source_tb = "dmp_uc_tags_m_bh"
    source_tb_sr = "dmp_ci_user_m_bh"
    source_flow_loc = "dmp_uc_otags_m_bh"
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
    #有标签用户
    hivesql = '''use %(HIVE_DATABASE)s;
    select count(distinct s_phone_no) from %(source_tb_sr)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s' ''' % vars()
    default_file = current_path+"/"+province+'_'+provider+'_today_user_number'
    print hivesql
    os.system('''hive -e "%(hivesql)s" > %(default_file)s''' % vars())

    all_user_num = 0
    for line in open(default_file):
        all_user_num = int(line.strip())
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s partition(provider,province,net_type,day_id,module)
        select kpi,value,provider,province,net_type,day_id,module from (
            select  concat_ws('|','uc_avg','uc_avg_1','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                    count(*)/%(all_user_num)s as value,
                    provider,province,'mobile' as net_type,'%(ARG_OPTIME)s' as day_id,'uc_avg' as module
            from %(source_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            group by provider,province,day_id

            union all

            select  concat_ws('|','uc_avg',name,'','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from (
                select mix_m_uid,province,provider,day_id,
                        case when value_type_id = 1 then 'uc_avg_2'
                        when value_type_id= 2 then 'uc_avg_3'
                        else '' end as name
                from %(source_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s' and value != ''
                group by mix_m_uid,value,province,provider,day_id,value_type_id
            ) aa where name != '' group by name,province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_4','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_tb)s where app_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by app_id,mix_m_uid,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_5','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_tb)s where cont_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by cont_id,mix_m_uid,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_7','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_flow_loc)s where sa_id ='loc_id' and tag_index != '' and  day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,tag_index,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_8','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_tb)s where site_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,site_id,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_9','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_tb)s where action_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,action_id,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_10','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_flow_loc)s where sa_id ='device_model' and tag_index != '' and  day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,tag_index,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_11','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_flow_loc)s where sa_id ='device_type' and tag_index != '' and  day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,tag_index,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_12','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_flow_loc)s where sa_id ='device_os' and tag_index != '' and  day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,tag_index,province,provider,day_id
            ) aa group by province,provider,day_id

            union all

            select  concat_ws('|','uc_avg','uc_avg_13','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day')  as kpi,
                count(*)/%(all_user_num)s as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc_avg' as module
            from(
               select mix_m_uid,province,provider,day_id from %(source_flow_loc)s where sa_id ='device_browser' and tag_index != '' and  day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
               group by mix_m_uid,tag_index,province,provider,day_id
            ) aa group by province,provider,day_id
        ) kk
    ''' % vars())

    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
