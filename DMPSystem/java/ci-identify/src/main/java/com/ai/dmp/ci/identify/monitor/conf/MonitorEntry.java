package com.ai.dmp.ci.identify.monitor.conf;

/**
 * Created by yulei on 2015/11/2.
 */
public class MonitorEntry {
    public String module;
    public String subidKey;
    public String subidValue;
    public String ruleType;
    public String rule;

    public MonitorEntry(String module, String subidKey, String subidValue) {
        this.module = module;
        this.subidKey = subidKey;
        this.subidValue = subidValue;
    }

    public MonitorEntry(String module, String subidKey, String subidValue, String ruleType, String rule) {
        this.module = module;
        this.subidKey = subidKey;
        this.subidValue = subidValue;
        this.ruleType = ruleType;
        this.rule = rule;
    }

    @Override
    public String toString() {
        return "MonitorEntry{" +
                "module='" + module + '\'' +
                ", subidKey='" + subidKey + '\'' +
                ", subidValue='" + subidValue + '\'' +
                ", ruleType='" + ruleType + '\'' +
                ", rule='" + rule + '\'' +
                '}';
    }
}
