package com.ai.dmp.ci.identify.conf;

import java.io.InputStream;
import java.util.Date;

import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.util.CIConst;
import com.opensymphony.module.propertyset.PropertySet;
import com.opensymphony.module.propertyset.PropertySetManager;
import com.opensymphony.module.propertyset.xml.XMLPropertySet;

/**
 * config.xml配置文件处理类
 *
 * @author yulei
 */
public class AbstractConfig {
    private static Logger log = Logger.getLogger(AbstractConfig.class);
    public static PropertySet PS;

    static {
        try {
            PS = PropertySetManager.getInstance("xml", null);  //将数据存储在xml中

            //加载基础配置文件
            InputStream in = Config.class.getClassLoader().getResourceAsStream("config/config.xml");
            try {
                ((XMLPropertySet) PS).load(in);
            } catch (Exception e) {
                log.error("加载config/config.xml配置文件失败!\n" + e.getMessage(), e);
            } finally {
                close(in);
            }

            //加载默认配置文件
            InputStream defaultIn = Config.class.getClassLoader().getResourceAsStream("config/default/config.xml");
            try {
                ((XMLPropertySet) PS).load(defaultIn);
            } catch (Exception e) {
                log.error("加载config/default/config.xml配置文件失败!\n" + e.getMessage(), e);
            } finally {
                close(defaultIn);
            }

            //加载各省不同的配置文件
            String key = CIMainParam.provider + "." + CIMainParam.province + "." + CIMainParam.nettype;
            String config = getString(key);
            if (StringUtils.isEmpty(config)) {
                log.fatal("没有指定配置文件：" + key);
                System.exit(1);
            }
            InputStream gsIn = Config.class.getClassLoader().
                    getResourceAsStream("config/" + config);
            try {
                ((XMLPropertySet) PS).load(gsIn);
            } catch (Exception e) {
                log.error("加载config/" + config + ",配置文件失败!\n" + e.getMessage(), e);
            } finally {
                close(gsIn);
            }

            //由于配置文件配置不能配置包括&的参数，所以将参数直接写死在代码
            String param = "?rewriteBatchedStatements=true&cachePrepStmts=true&useServerPrepStmts=true&useUnicode=true&autoReconnect=true&failOverReadOnly=false&characterEncoding=UTF-8&connectTimeout=0";
            param = "?useUnicode=true&autoReconnect=true&failOverReadOnly=false&characterEncoding=UTF-8&connectTimeout=0";
            String ruleDBUrl = PS.getString(CIConst.Config.RULE_DB_URL);
            PS.setString(CIConst.Config.RULE_DB_URL, ruleDBUrl + param);
        } catch (Exception e) {
            log.error("加载配置文件失败程序退出执行!" + e.getMessage(), e);
            System.exit(1);
        }

    }

    public static String getString(String paramString) {
        return PS.getString(paramString);
    }

    public static int getInt(String paramString) {
        return PS.getInt(paramString);
    }

    public static boolean getBoolean(String paramString) {
        return PS.getBoolean(paramString);
    }

    public static Date getDate(String paramString) {
        return PS.getDate(paramString);
    }

    public static boolean exists(String paramString) {
        return PS.exists(paramString);
    }

    private static void close(InputStream in) {
        try {
            if (in != null) {
                in.close();
            }
        } catch (Exception e) {
            log.error("关闭文件错误！" + e.getMessage(), e);
        }
    }
}
