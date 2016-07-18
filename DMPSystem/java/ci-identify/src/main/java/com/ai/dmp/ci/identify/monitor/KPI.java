package com.ai.dmp.ci.identify.monitor;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.mr.CIMainParam;

/**
 * Created by yulei on 2015/11/2.
 */
public class KPI {
    private static final String DEFAULT_VALUE = "";

    private String module = DEFAULT_VALUE;
    private String subid = DEFAULT_VALUE;
    private String ruleType = DEFAULT_VALUE;
    private String rule = DEFAULT_VALUE;
    private String range = DEFAULT_VALUE;
    private String provience = CIMainParam.province;
    private String provider = CIMainParam.provider;
    private String nettype = CIMainParam.nettype;
    private String recoredtype = DEFAULT_VALUE;
    private String usertype = DEFAULT_VALUE;
    private String staticstype = "hour";

    public KPI(Result result) {
        this.range = result.get(CIConst.ResultColName.HOUR_ID);
        String type = result.get(CIConst.ResultColName.TYPE);
        this.recoredtype = CIConst.Type.AD.equals(type) ? CIConst.Type.AD : CIConst.Type.CLICK;
    }

    public KPI(Result result, String module, String subid) {
        this(result);
        this.module = module;
        this.subid = subid;
    }

    public KPI(Result result, String module, String subid, String ruleType, String rule) {
        this(result, module, subid);
        this.ruleType = ruleType;
        this.rule = rule;
    }

    public KPI(String module, String subid, String ruleType, String rule,String range,String recoredtype) {
        this.module = module;
        this.subid = subid;
        this.ruleType = ruleType;
        this.rule = rule;
        this.range = range;
        this.recoredtype = recoredtype;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(module).append(CIConst.Separator.VerticalLine);
        sb.append(subid).append(CIConst.Separator.VerticalLine);
        sb.append(ruleType).append(CIConst.Separator.VerticalLine);
        sb.append(rule).append(CIConst.Separator.VerticalLine);
        sb.append(range).append(CIConst.Separator.VerticalLine);
        sb.append(provience).append(CIConst.Separator.VerticalLine);
        sb.append(provider).append(CIConst.Separator.VerticalLine);
        sb.append(nettype).append(CIConst.Separator.VerticalLine);
        sb.append(recoredtype).append(CIConst.Separator.VerticalLine);
        sb.append(usertype).append(CIConst.Separator.VerticalLine);
        sb.append(staticstype);
        return sb.toString();
    }

    public void setModule(String module) {
        this.module = module;
    }

    public void setSubid(String subid) {
        this.subid = subid;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }
}
