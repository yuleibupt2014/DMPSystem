package com.ai.dmp.ci.identify.core.matcher.loc;

import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/20.
 */
public class LocRuleCacheBean extends BaseRuleCacheBean {
    private String lngKey;
    private Pattern lngRegexPattern;
    private String latKey;
    private Pattern latRegexPattern;
    private String prefix;

    public String getLngKey() {
        return lngKey;
    }

    public void setLngKey(String lngKey) {
        this.lngKey = lngKey;
    }

    public Pattern getLngRegexPattern() {
        return lngRegexPattern;
    }

    public void setLngRegexPattern(Pattern lngRegexPattern) {
        this.lngRegexPattern = lngRegexPattern;
    }

    public String getLatKey() {
        return latKey;
    }

    public void setLatKey(String latKey) {
        this.latKey = latKey;
    }

    public Pattern getLatRegexPattern() {
        return latRegexPattern;
    }

    public void setLatRegexPattern(Pattern latRegexPattern) {
        this.latRegexPattern = latRegexPattern;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
