#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   
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

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
provider = sys.argv[1]  #运营商  如: lt/dx/dxy/yd
province = sys.argv[2]  #省份    如：jiangsu/beijing/shanghai  等
nettype = sys.argv[3]   #数据类型  如：mobile/adsl
startHourId = sys.argv[4]  #开始时间
endHourId = sys.argv[5]  #结束时间

#hadoop相关参数
hadoop_params=["/user/gdpi/public/sada_gdpi_click.password=GWDPI-SH",
               "/user/gdpi/public/sada_gdpi_adcookie.password=CKDPI-SH"]

dates = startHourId[:8]
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    args0 =  provider + "," + province + "," + nettype + "," + startHourId + "," + endHourId
    args1 = HIVE_HOME_PATH + "," + HIVE_DATABASE

    #拼接hadoop参数
    args2 = ""
    for i in range(len(hadoop_params)):
        args2+=hadoop_params[i]+","
    if(args2 != ""):
        args2 = args2[:-1]

    cmd = "hadoop jar " + JAR_DMP_CI + " " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    print cmd
    os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
