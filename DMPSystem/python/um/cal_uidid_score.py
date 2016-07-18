#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback

if __name__ == '__main__':
    try:
        for line in sys.stdin:
            temp = line.strip().split("\t")
            #m_id
            #phone_list
            #mflag
            arr = temp[1][:-1][1:].replace('"', '').split(',')
            if len(arr) > 1000: continue
            score = 1.0/len(arr)
            for value in arr:
                print temp[0]+"\t"+value+"\t"+str(score)+"\t"+temp[2]
    except Exception, e:
        print traceback.format_exc()
