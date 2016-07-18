package com.ai.dmp.ci.identify.monitor.conf;

import com.opensymphony.module.propertyset.PropertySet;
import com.opensymphony.module.propertyset.PropertySetManager;
import com.opensymphony.module.propertyset.xml.XMLPropertySet;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Date;

/**
 * monitorconfig.xml配置文件处理类
 *
 * @author yulei
 */
public abstract class AbstractMNConfig {
    private static Logger log = Logger.getLogger(AbstractMNConfig.class);
    public static final PropertySet PS;

    static {
        PS = PropertySetManager.getInstance("xml", null);

        //加载基础配置文件
        InputStream in = MNConfig.class.getClassLoader().getResourceAsStream("config/mnconfig.xml");
        try {
            ((XMLPropertySet) PS).load(in);
        } catch (Exception e) {
            log.error("加载config/mnconfig.xml配置文件失败!\n" + e.getMessage(), e);
        } finally {
            close(in);
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

    private static void close(InputStream in ) {
        try {
            if (in != null) {
                in.close();
            }
        } catch (Exception e) {
            log.error("关闭文件错误！" + e.getMessage(), e);
        }
    }
}
