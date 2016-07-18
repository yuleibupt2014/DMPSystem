package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleContActionBean extends BaseMatchBean {
    private String urlContains;//URL包含的字符串
    private String urlKey;
    private String urlRegex;//URL的匹配正则表达式
    private String refContains;//REF包含的字符串
    private String refKey;
    private String refRegex;//REF的匹配正则表达式
    private String contId;  //内容分类ID
    private String actionId;  //行为动作ID
    private String valueTypeId; //行为对象类型
    private String prefix; //行为对象值的前缀

    public String getUrlContains() {
        return urlContains;
    }

    public void setUrlContains(String urlContains) {
        this.urlContains = urlContains;
    }

    public String getUrlKey() {
        return urlKey;
    }

    public void setUrlKey(String urlKey) {
        this.urlKey = urlKey;
    }

    public String getUrlRegex() {
        return urlRegex;
    }

    public void setUrlRegex(String urlRegex) {
        this.urlRegex = urlRegex;
    }

    public String getRefContains() {
        return refContains;
    }

    public void setRefContains(String refContains) {
        this.refContains = refContains;
    }

    public String getRefKey() {
        return refKey;
    }

    public void setRefKey(String refKey) {
        this.refKey = refKey;
    }

    public String getRefRegex() {
        return refRegex;
    }

    public void setRefRegex(String refRegex) {
        this.refRegex = refRegex;
    }

    public String getContId() {
        return contId;
    }

    public void setContId(String contId) {
        this.contId = contId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public String getValueTypeId() {
        return valueTypeId;
    }

    public void setValueTypeId(String valueTypeId) {
        this.valueTypeId = valueTypeId;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "DimCiRuleContActionBean{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", urlContains='" + urlContains + '\'' +
                ", urlKey='" + urlKey + '\'' +
                ", urlRegex='" + urlRegex + '\'' +
                ", refContains='" + refContains + '\'' +
                ", refKey='" + refKey + '\'' +
                ", refRegex='" + refRegex + '\'' +
                ", contId='" + contId + '\'' +
                ", actionId='" + actionId + '\'' +
                ", valueTypeId='" + valueTypeId + '\'' +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
