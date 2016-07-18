package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleAppBean extends BaseMatchBean {
    private String urlContains;//URL包含的字符串
    private String urlRegex;//URL的匹配正则表达式
    private String uaContains;//UA包含的字符串
    private String uaRegex;//UA的匹配正则表达式
    private String appId; //匹配的APP ID

    public String getUrlContains() {
        return urlContains;
    }

    public void setUrlContains(String urlContains) {
        this.urlContains = urlContains;
    }

    public String getUrlRegex() {
        return urlRegex;
    }

    public void setUrlRegex(String urlRegex) {
        this.urlRegex = urlRegex;
    }

    public String getUaContains() {
        return uaContains;
    }

    public void setUaContains(String uaContains) {
        this.uaContains = uaContains;
    }

    public String getUaRegex() {
        return uaRegex;
    }

    public void setUaRegex(String uaRegex) {
        this.uaRegex = uaRegex;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public String toString() {
        return "DimCiRuleAppBean{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", urlContains='" + urlContains + '\'' +
                ", urlRegex='" + urlRegex + '\'' +
                ", uaContains='" + uaContains + '\'' +
                ", uaRegex='" + uaRegex + '\'' +
                ", appId='" + appId + '\'' +
                '}';
    }
}
