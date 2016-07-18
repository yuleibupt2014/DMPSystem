package com.ai.dmp.ci.identify.core.db.dao;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIDaoImpl;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIHdfsDaoImpl;
import com.ai.dmp.ci.identify.mr.CIMap;

/**
 * Created by yulei on 2015/11/6.
 */
public class DaoUtil {
    public static ICIDao ciDao = null;//数据库操作对象

    static {
        if (Config.isRuleDbHdfs()) {//是否启用从HDFS加载数据
            ciDao = new CIHdfsDaoImpl(CIMap.conf);
        } else {
            ciDao = new CIDaoImpl(); //否则从mysql中加载数据
        }
    }
}
