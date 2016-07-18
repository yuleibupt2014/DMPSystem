package com.ai.dmp.ci.common.util;

/**
 * 定义监控相关的所有常量
 *
 */
public class CIMonitorConst {

    /**
     * <b>config.xml配置文件属性名称定义</b>
     * <ul>
     * <li>修改时间:2015-4-23</li>
     * <li>修改内容:添加IS_TOKEN_MATCHER_HOST_LEFT_LIKE属性</li>
     * </ul>
     *
     */
    public class Config {

        // 数据源监控subid
        public final static String DPI_SUBID = "dpi.subid";
        public final static String DPI_RULE = "dpi.rule";
        public final static String DPI_RULETYPE = "dpi.ruletype";
        //数据清洗监控rule
        public final static String DC_SUBID = "dc.subid";
        public final static String DC_RULE = "dc.rule";
        public final static String DC_RULETYPE = "dc.ruletype";
        //内容识别监控 3.4.2.3.1	行为标签识别
        public final static String CI_TAG_SUBID = "ci.tag.subid";
        public final static String CI_TAG_RULE = "ci.tag.rule";
        public final static String CI_TAG_RULETYPE = "ci.tag.ruletype";
        //内容识别监控3.4.2.3.2	用户id识别
        public final static String CI_U_SUBID = "ci.u.subid";
        public final static String CI_U_RULE = "ci.u.rule";
        public final static String CI_U_RULETYPE = "ci.u.ruletype";
        //内容识别监控3.4.2.3.3	行为标签识别规则
        public final static String CI_TAG_R_SUBID = "ci.tag.r.subid";
        public final static String CI_TAG_R_RULE = "ci.tag.r.rule";
        public final static String CI_TAG_R_RULETYPE = "ci.tag.r.ruletype";
        //内容识别监控3.4.2.3.4	用户id识别规则
        public final static String CI_U_R_SUBID = "ci.u.r.subid";
        public final static String CI_U_R_RULE = "ci.u.r.rule";
        public final static String CI_U_R_RULETYPE = "ci.u.r.ruletype";

        public final static String IS_MONITOR_HOUR = "is.monitor.hour";

    }



}
