package com.ai.dmp.ci.identify.core.matcher.userflag;

import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/20.
 */
public class UserFlagRuleCacheBean extends BaseRuleCacheBean {
    private String flagType;//用户标志类型，cookie_id、user_name、email、phone_no、imei、imsi、mac、idfa、android_id
    private String urlKey;  //URL中的参数名称
    private Pattern urlRegexPattern;  //URL匹配正则
    private String cookieKey;  //URL中的参数名称
    private Pattern cookieRegexPattern;  //URL匹配正则
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

    public Pattern getUrlRegexPattern() {
        return urlRegexPattern;
    }

    public void setUrlRegexPattern(Pattern urlRegexPattern) {
        this.urlRegexPattern = urlRegexPattern;
    }

    public String getCookieKey() {
        return cookieKey;
    }

    public void setCookieKey(String cookieKey) {
        this.cookieKey = cookieKey;
    }

    public Pattern getCookieRegexPattern() {
        return cookieRegexPattern;
    }

    public void setCookieRegexPattern(Pattern cookieRegexPattern) {
        this.cookieRegexPattern = cookieRegexPattern;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
