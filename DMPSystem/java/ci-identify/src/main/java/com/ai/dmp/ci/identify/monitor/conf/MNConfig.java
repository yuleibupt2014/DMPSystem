package com.ai.dmp.ci.identify.monitor.conf;

import com.ai.dmp.ci.common.util.StringUtil;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yulei on 2015/11/2.
 */
public class MNConfig extends AbstractMNConfig {
    private static Logger log = Logger.getLogger(MNConfig.class);

    public static final boolean isMonitor = PS.getBoolean("is.monitor.hour");//是否开启监控

    private static final String DPI_SUBID = "dpi_subid";
    private static final String CI_TAGS_SUBID = "ci_tag_subid";
    private static final String CI_U_SUBID = "ci_u_subid";
    private static final String CI_TAGS_R_SUBID = "ci_tag_r_subid";
    private static final String CI_U_R_SUBID = "ci_u_r_subid";

    private static final String _Subid = "_subid";
    private static final String _Ruletype = "_ruletype";

    private static List<MonitorEntry> entryList = new ArrayList<MonitorEntry>();//缓存配置！！！重要，明白entrylist里面装的是什么

    public static void main(String[] args) {
        for (MonitorEntry entry : entryList) {
            System.out.println(entry);
        }
        System.out.println(isMonitor);
    }

    static {
        init();
    }

    private static void init() {
        initSubid(DPI_SUBID);
        initSubid(CI_TAGS_SUBID);
        initSubid(CI_U_SUBID);
        initSubid(CI_TAGS_R_SUBID);
        initSubid(CI_U_R_SUBID);
    }

    private static void initSubid(String subidName) {
        try {
            String module = subidName.substring(0, subidName.length() - _Subid.length());
            String subidStr = PS.getString(subidName);
            String[] subids = subidStr.split(",");
            for (String subid : subids) {
                String[] kv = subid.split("=");
                String ruleTypeStr = PS.getString(module + _Ruletype);
                if (!StringUtil.isEmpty(ruleTypeStr)) {//有规则类型的情况
                    String[] ruleTypes = ruleTypeStr.split(",");
                    for (String ruleType : ruleTypes) {
                        String[] rule = ruleType.split("=");
                        MonitorEntry entry = new MonitorEntry(module, kv[0], null, rule[0], rule[1]);
                        entryList.add(entry);
                    }
                } else {
                    MonitorEntry entry = new MonitorEntry(module, kv[0], kv[1]);
                    entryList.add(entry);
                }
            }
        } catch (Exception e) {
            log.error("加载监控配置失败！程序退出！\n" + e.getMessage(), e);
            System.exit(1);
        }
    }


    public static List<MonitorEntry> getMonitorEntryList() {
        return entryList;
    }
}
