#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

total=1L
values=[]
value_str=[]
lastKeyPrefix=''
isHour=False
max_num=1
num=0

value_sep=u'\u0003'
tab_sep='\t'

#
def toPrint(key,values):
    global isHour
    if(isHour):
        hour_id = datatime
        day_id  = datatime[0:8]
        print key +tab_sep+ values[1] +tab_sep+ day_id + tab_sep + hour_id+tab_sep + values[3] +tab_sep+values[4]
    else:
        day_id  = datatime[0:8]
        print key +tab_sep+ values[1] +tab_sep+ day_id +tab_sep + values[3] +tab_sep+values[4]
#main
if __name__ == '__main__':
    for lines in sys.stdin:
        try:
            #lines: pri(at)  value  datatime[20141008|2014100818]
            lines=lines[:-1]
            words = lines.split('\t')
            
            datatime = words[2]               ##########
            keyPrefix = words[0]+"_"+datatime #########
            if(len(datatime)==10):
                isHour=True
            value = words[1]
            value_str.append(value)
            tmp = value_sep.join(value_str)
            tmp_len=len(tmp)
            words[1] = tmp
            key = keyPrefix+'-'+str(total)
            num = num + 1
            if(keyPrefix != lastKeyPrefix and lastKeyPrefix != ''):#total = 1L
                key = keyPrefix+'-'+str(total)
                toPrint(key, words)
                total=total+1
                value_str=[]
                lastKeyPrefix = keyPrefix
                num = 0
                continue
            if(num == max_num):
                num = 0
                key = keyPrefix+'-'+str(total)
                toPrint(key,words)
                total=total+1
                value_str=[]
                lastKeyPrefix = keyPrefix
                continue
            #toPrint(key,words)
            #total=total+1
            #lastKeyPrefix = keyPrefix
        except Exception as e:
            #print e
            continue
    if(num>0):
        toPrint(key,words)