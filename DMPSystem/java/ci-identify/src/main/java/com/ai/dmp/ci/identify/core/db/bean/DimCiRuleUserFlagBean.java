package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleUserFlagBean extends BaseMatchBean {
    private String flagType;//用户标志类型，cookie_id、user_name、email、phone_no、imei、imsi、mac、idfa、android_id
    private String urlKey;  //URL中的参数名称
    private String urlRegex;  //URL匹配正则
    private String cookieKey;  //URL中的参数名称
    private String cookieRegex;  //URL匹配正则
    private String prefix;//前缀，比如qq_

    public String getFlagType() {
        return flagType;
    }

    public void setFlagType(String flagType) {
        this.flagType = flagType;
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

    public String getCookieKey() {
        return cookieKey;
    }

    public void setCookieKey(String cookieKey) {
        this.cookieKey = cookieKey;
    }

    public String getCookieRegex() {
        return cookieRegex;
    }

    public void setCookieRegex(String cookieRegex) {
        this.cookieRegex = cookieRegex;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "DimCiRuleUserFlagBean{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", flagType='" + flagType + '\'' +
                ", urlKey='" + urlKey + '\'' +
                ", urlRegex='" + urlRegex + '\'' +
                ", cookieKey='" + cookieKey + '\'' +
                ", cookieRegex='" + cookieRegex + '\'' +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
