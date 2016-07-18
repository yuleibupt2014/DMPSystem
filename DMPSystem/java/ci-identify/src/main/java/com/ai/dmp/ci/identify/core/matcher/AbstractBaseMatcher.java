package com.ai.dmp.ci.identify.core.matcher;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.dao.DaoUtil;
import com.ai.dmp.ci.identify.core.db.dao.ICIDao;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIDaoImpl;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIHdfsDaoImpl;
import com.ai.dmp.ci.identify.mr.CIMap;
import org.apache.log4j.Logger;

public abstract class AbstractBaseMatcher {
    protected static Logger log = Logger.getLogger(AbstractBaseMatcher.class);

    protected static ICIDao ciDao = DaoUtil.ciDao;

}


