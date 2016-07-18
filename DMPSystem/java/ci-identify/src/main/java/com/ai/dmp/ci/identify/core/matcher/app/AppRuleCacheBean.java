package com.ai.dmp.ci.identify.core.matcher.app;

import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/20.
 */
public class AppRuleCacheBean extends BaseRuleCacheBean {
    private String urlContains;//URL包含的字符串
    private Pattern urlRegexPattern;//URL的匹配正则表达式
    private String uaContains;//UA包含的字符串
    private Pattern uaRegexPattern;//UA的匹配正则表达式
    private String appId; //匹配的APP ID
    private String contId;

    public String getUrlContains() {
        return urlContains;
    }

    public void setUrlContains(String urlContains) {
        this.urlContains = urlContains;
    }

    public Pattern getUrlRegexPattern() {
        return urlRegexPattern;
    }

    public void setUrlRegexPattern(Pattern urlRegexPattern) {
        this.urlRegexPattern = urlRegexPattern;
    }

    public String getUaContains() {
        return uaContains;
    }

    public void setUaContains(String uaContains) {
        this.uaContains = uaContains;
    }

    public Pattern getUaRegexPattern() {
        return uaRegexPattern;
    }

    public void setUaRegexPattern(Pattern uaRegexPattern) {
        this.uaRegexPattern = uaRegexPattern;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getContId() {
        return contId;
    }

    public void setContId(String contId) {
        this.contId = contId;
    }
}
