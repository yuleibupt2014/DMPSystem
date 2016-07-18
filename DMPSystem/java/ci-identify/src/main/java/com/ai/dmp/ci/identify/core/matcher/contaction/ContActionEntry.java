package com.ai.dmp.ci.identify.core.matcher.contaction;

/**
 * Created by yulei on 2015/10/30.
 */
public class ContActionEntry {
    private String contId;
    private String actionId;
    private String valueTypeId;
    private String value;
    private String contActionRuleId;
    private String contAppRuleId;

    public ContActionEntry(String contId, String actionId, String valueTypeId, String value, String contActionRuleId,String contAppRuleId) {
        this.contId = contId == null ? "" : contId;
        this.actionId = actionId  == null ? "" : actionId;
        this.valueTypeId = valueTypeId  == null ? "" : valueTypeId;
        this.value = value  == null ? "" : value;
        this.contActionRuleId = contActionRuleId  == null ? "" : contActionRuleId;
        this.contAppRuleId = contAppRuleId  == null ? "" : contAppRuleId;
    }

    public String getContId() {
        return contId;
    }

    public String getActionId() {
        return actionId ;
    }

    public String getValueTypeId() {
        return valueTypeId;
    }

    public String getValue() {
        return value;
    }

    public String getContActionRuleId() {
        return contActionRuleId;
    }

    public String getContAppRuleId() {
        return contAppRuleId;
    }

    @Override
    public String toString() {
        return "ContActionEntry{" +
                "contId='" + contId + '\'' +
                ", actionId='" + actionId + '\'' +
                ", valueTypeId='" + valueTypeId + '\'' +
                ", value='" + value + '\'' +
                ", contActionRuleId='" + contActionRuleId + '\'' +
                ", contAppRuleId='" + contAppRuleId + '\'' +
                '}';
    }
}
