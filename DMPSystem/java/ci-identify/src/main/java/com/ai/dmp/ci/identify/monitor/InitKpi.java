package com.ai.dmp.ci.identify.monitor;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.DaoUtil;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import com.ai.dmp.ci.identify.core.matcher.terminal.TerminalRuleCacheBean;
import com.ai.dmp.ci.identify.monitor.conf.MNConfig;
import com.ai.dmp.ci.identify.monitor.conf.MonitorEntry;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yulei on 2015/11/6.
 */
public class InitKpi {
    private static Logger log = Logger.getLogger(InitKpi.class);

    // 输出字段分隔符
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);
    private static Text keyText = new Text();
    private static Text valueText0 = new Text("0");

    private List<KPI> allKpiList = new ArrayList<KPI>();

    public InitKpi() {
        initAllKpi0();
    }

    /**
     * 初始化所有指标值为0
     *
     * @return
     * @throws Exception
     */
    public void initAllKpi0() {  //entryList里面装的是MonitorEntry实例，是mnconfig文件中解析出来的
        try {
            List<String> hourIds = CIMainParam.getHourIds();
            List<MonitorEntry> entryList = MNConfig.getMonitorEntryList();
            String[] types = new String[]{CIConst.Type.AD, CIConst.Type.CLICK};
            for (String hourId : hourIds) {
                for (String type : types) {
                    for (MonitorEntry entry : entryList) {
                        if (!StringUtil.isEmpty(entry.ruleType) && !StringUtil.isEmpty(entry.rule)) { //计算根据哪些规则识别
                            initRuleKpi(allKpiList, entry.module, entry.subidKey, entry.ruleType, hourId, type);
                        } else {
                            addKpi(allKpiList, entry.module, entry.subidKey, "", "", hourId, type);
                        }
                    }

                    //以下指标未在配置文件配置
                    initRuleKpi(allKpiList, "ci_tag_r", "ci_tag_r_1", "cont_action", hourId, type);
                    initRuleKpi(allKpiList, "ci_tag_r", "ci_tag_r_1", "cont_app", hourId, type);

                    //dpi数据源指标
                    addKpi(allKpiList, "dpi", "dpi_1", "", "", hourId, type);

                    //数据清洗指标
                    addKpi(allKpiList, "dc", "dc_1", "", "1", hourId, type);
                    addKpi(allKpiList, "dc", "dc_1", "", "2", hourId, type);
                    addKpi(allKpiList, "dc", "dc_1", "", "3", hourId, type);
                    addKpi(allKpiList, "dc", "dc_1", "", "4", hourId, type);

                    //内容分类相关指标
                    addKpi(allKpiList, "ci_tag", "ci_tag_1", "", "", hourId, type);
                    addKpi(allKpiList, "ci_tag", "ci_tag_2", "", "", hourId, type);
                    addKpi(allKpiList, "ci_tag", "ci_tag_3", "", "", hourId, type);
                    addKpi(allKpiList, "ci_tag", "ci_tag_5", "", "", hourId, type);
                    addKpi(allKpiList, "ci_tag", "ci_tag_6", "", "", hourId, type);
                    addKpi(allKpiList, "ci_tag", "ci_tag_7", "", "", hourId, type);
                }
            }
        } catch (Exception e) {
            log.error("初始化(设置为0)所有指标错误！" + e.getMessage(), e);
        }
    }

    /**
     * 输出所有初始化指标
     *
     * @param context
     * @throws Exception
     */
    public void writeInitKpi0(Mapper.Context context) throws Exception {
        for (KPI kpi : allKpiList) {
            keyText.set(CIConst.OutputPri.MONITOR + SEPARATOR + kpi.toString());
            context.write(keyText, valueText0);  //valueText0 = "0"
        }
        allKpiList = null;
    }

    /**
     * 将某个指标添加到集合中
     *
     * @param allKpiList
     * @param module
     * @param subid
     * @param ruleType
     * @param rule
     * @param range
     * @param recoredtype
     */
    private void addKpi(List<KPI> allKpiList, String module, String subid, String ruleType,
                        String rule, String range, String recoredtype) {
        KPI kpi = new KPI(module, subid, ruleType, rule, range, recoredtype);
        allKpiList.add(kpi);
    }

    /**
     * 先查询规则，再构建指标，并将指标放入集合中
     *
     * @param allKpiList
     * @param module
     * @param subid
     * @param ruleType
     * @param range
     * @param recoredtype
     * @throws Exception
     */
    private void initRuleKpi(List<KPI> allKpiList, String module, String subid,
                             String ruleType, String range, String recoredtype) throws Exception {
        if ("cont_action".equals(ruleType)) {
            addRuleKpi(allKpiList, MatcherUtil.contActionRuleList, module, subid, ruleType, range, recoredtype);
        } else if ("cont_app".equals(ruleType)) {
            addRuleKpi(allKpiList, MatcherUtil.appRuleList, module, subid, ruleType, range, recoredtype);
        } else if ("app".equals(ruleType)) {
            addRuleKpi(allKpiList, MatcherUtil.appRuleList, module, subid, ruleType, range, recoredtype);
        } else if ("loc".equals(ruleType)) {
            addRuleKpi(allKpiList, MatcherUtil.locRuleList, module, subid, ruleType, range, recoredtype);
        } else if ("site".equals(ruleType)) {
            addRuleKpi(allKpiList, MatcherUtil.siteRuleList, module, subid, ruleType, range, recoredtype);
        } else if ("user_name".equals(ruleType) ||
                "email".equals(ruleType) ||
                "phone_no".equals(ruleType) ||
                "imei".equals(ruleType) ||
                "imsi".equals(ruleType) ||
                "mac".equals(ruleType) ||
                "idfa".equals(ruleType) ||
                "android_id".equals(ruleType)) { //用户ID管理
            addUserFlagRuleKpi(allKpiList, MatcherUtil.userFlagList, module, subid, ruleType, range, recoredtype);
        } else if ("device_model".equals(ruleType) ||
                "device_type".equals(ruleType) ||
                "device_os".equals(ruleType) ||
                "device_browser".equals(ruleType)) { //终端识别
            addTerminalRuleKpi(allKpiList, MatcherUtil.terminalList, module, subid, ruleType, range, recoredtype);
        }
    }

    private void addRuleKpi(List<KPI> allKpiList, List ruleList, String module, String subid,
                            String ruleType, String range, String recoredtype) {
        for (Object bean : ruleList) {
            BaseMatchBean baseBean = (BaseMatchBean) bean;
            KPI kpi = new KPI(module, subid, ruleType, String.valueOf(baseBean.getId()), range, recoredtype);
            allKpiList.add(kpi);
        }
    }

    private void addUserFlagRuleKpi(List<KPI> allKpiList, List ruleList, String module, String subid,
                                    String ruleType, String range, String recoredtype) {
        for (Object bean : ruleList) {
            DimCiRuleUserFlagBean userFlagCacheBean = (DimCiRuleUserFlagBean) bean;
            if (ruleType.equals(userFlagCacheBean.getFlagType())) {
                KPI kpi = new KPI(module, subid, ruleType, String.valueOf(userFlagCacheBean.getId()), range, recoredtype);
                allKpiList.add(kpi);
            }
        }
    }

    private void addTerminalRuleKpi(List<KPI> allKpiList, List ruleList, String module, String subid,
                                    String ruleType, String range, String recoredtype) {
        for (Object bean : ruleList) {
            DimCiRuleTerminalBean terminalBean = (DimCiRuleTerminalBean) bean;
            if (ruleType.equals(terminalBean.getTerminalFlag())) {
                KPI kpi = new KPI(module, subid, ruleType, String.valueOf(terminalBean.getId()), range, recoredtype);
                allKpiList.add(kpi);
            }
        }
    }
}
