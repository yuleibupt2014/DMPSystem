package com.ai.dmp.ci.identify.monitor;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleAppBean;
import com.ai.dmp.ci.identify.core.db.dao.DaoUtil;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIDaoImpl;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIHdfsDaoImpl;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import com.ai.dmp.ci.identify.monitor.conf.MNConfig;
import com.ai.dmp.ci.identify.monitor.conf.MonitorEntry;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import com.ai.dmp.ci.identify.mr.CIMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yulei on 2015/11/2.
 */
public class MonitorHandler {
    // 输出字段分隔符
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);

    Text keyText = new Text();
    Text valueText1 = new Text("1");

    boolean isInit = false;
    InitKpi initKpi = new InitKpi();

    public void handle(Result result, Mapper.Context context) throws Exception {
        if (!isInit) {
            initKpi.writeInitKpi0(context); //第一次初始化所有指标计数为0，主要是为了将所有指标都输出
            isInit = true;
        }

        //计算所有的指标
        List<KPI> kpiList = computeKPI(result);

        for (KPI kpi : kpiList) {
            keyText.set(CIConst.OutputPri.MONITOR + SEPARATOR + kpi.toString());
            context.write(keyText, valueText1);
        }
    }


    /**
     * 计算KPI
     *
     * @param result
     * @return
     */
    private List<KPI> computeKPI(Result result) {
        List<KPI> kpiList = new ArrayList<KPI>();
        KPI kpi = null;

        //总记录数
        String cleanRuleId = result.get(CIConst.ResultColName_RuleID.CLEAN_RULE_ID);
        if (!CIConst.CleanRuleId.RULE_1.equals(cleanRuleId)) {//记录格式不合法的记录，不计入总记录数
            kpi = new KPI(result, "dpi", "dpi_1");
            kpiList.add(kpi);
        }

        if (result.ifCleaned()) {
            //数据清洗
            kpi = new KPI(result, "dc", "dc_1");
            cleanRuleId = result.get(CIConst.ResultColName_RuleID.CLEAN_RULE_ID);
            kpi.setRule(cleanRuleId);
            kpiList.add(kpi);

            if (!CIConst.CleanRuleId.RULE_1.equals(cleanRuleId)) {//记录格式不正确的数据，只计入总记录数和清洗数，不计入其他指标
                monitorByConfig_S(result, kpiList);//计算缺失数相关的指标
            }
        } else {
            //根据配置文件进行监控
            monitorByConfig(result, kpiList);

            //复杂指标监控
            monitorComplexKpi(result, kpiList);
        }
        return kpiList;
    }


    /**
     * 复杂指标监控
     *
     * @param result
     * @param kpiList
     */
    private void monitorComplexKpi(Result result, List<KPI> kpiList) {
        String ciTag = "ci_tag";
        //清洗后记录数
        KPI kpi = null;
        kpi = new KPI(result, ciTag, "ci_tag_1");
        kpiList.add(kpi);

        List<ContActionEntry> contActionList = result.getContActionList();
        for (ContActionEntry entry : contActionList) {
            kpi = new KPI(result, ciTag, "ci_tag_2"); //访问操作识别记录数
            kpiList.add(kpi);

            //访问操作识别规则
            if (!StringUtil.isEmpty(entry.getContActionRuleId())) {
                kpi = new KPI(result, "ci_tag_r", "ci_tag_r_1", "cont_action", entry.getContActionRuleId());
                kpiList.add(kpi);
            } else if (!StringUtil.isEmpty(entry.getContAppRuleId())) { //根据App打内容分类标签
                kpi = new KPI(result, "ci_tag_r", "ci_tag_r_1", "cont_app", entry.getContAppRuleId());
                kpiList.add(kpi);
            }

            //关键字识别规则
            if (!StringUtil.isEmpty(entry.getValue()) && CIConst.ValueTypeId.KEYWORD_1.equals(entry.getValueTypeId())) {
                kpi = new KPI(result, ciTag, "ci_tag_3");
                kpiList.add(kpi);
            }

            // 商品识别记录数
            if (CIConst.ValueTypeId.GOODS_ID_2.equals(entry.getValueTypeId())) {
                kpi = new KPI(result, ciTag, "ci_tag_5");
                kpiList.add(kpi);
            }

            //内容识别记录数
            if (!StringUtil.isEmpty(entry.getContId())) {
                kpi = new KPI(result, ciTag, "ci_tag_6");
                kpiList.add(kpi);
            }

            //行为识别记录数
            if (!StringUtil.isEmpty(entry.getActionId())) {
                kpi = new KPI(result, ciTag, "ci_tag_9");
                kpiList.add(kpi);
            }
        }
    }

    /**
     * 根据配置文件进行监控
     *
     * @param result
     * @param kpiList
     */
    private void monitorByConfig(Result result, List<KPI> kpiList) {
        List<MonitorEntry> entryList = MNConfig.getMonitorEntryList();

        for (MonitorEntry entry : entryList) {
            if (!StringUtil.isEmpty(entry.subidValue) && entry.subidValue.startsWith("s_")) {
                if (StringUtil.isEmpty(result.get(entry.subidValue))) { //计算缺失指标
                    KPI kpi = new KPI(result, entry.module, entry.subidKey);
                    kpiList.add(kpi);
                }
            } else if (!StringUtil.isEmpty(entry.subidValue)) {
                if (!StringUtil.isEmpty(result.get(entry.subidValue))) { //计算识别指标
                    KPI kpi = new KPI(result, entry.module, entry.subidKey);
                    kpiList.add(kpi);
                }
            } else if (!StringUtil.isEmpty(entry.ruleType) && !StringUtil.isEmpty(entry.rule) &&
                    !StringUtil.isEmpty(result.get(entry.rule))) { //计算根据哪些规则识别
                KPI kpi = new KPI(result, entry.module, entry.subidKey, entry.ruleType, result.get(entry.rule));
                kpiList.add(kpi);
            }
        }
    }

    /**
     * 根据配置文件进行监控缺失数指标
     *
     * @param result
     * @param kpiList
     */
    private void monitorByConfig_S(Result result, List<KPI> kpiList) {
        List<MonitorEntry> entryList = MNConfig.getMonitorEntryList();
        for (MonitorEntry entry : entryList) {
            if (!StringUtil.isEmpty(entry.subidValue) && entry.subidValue.startsWith("s_")) {
                if (StringUtil.isEmpty(result.get(entry.subidValue))) { //计算缺失指标
                    KPI kpi = new KPI(result, entry.module, entry.subidKey);
                    kpiList.add(kpi);
                }
            }
        }
    }
}
