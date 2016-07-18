package com.ai.dmp.ci.identify.core.matcher.contaction;

import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/20.
 */
public class ContActionRuleCacheBean extends BaseRuleCacheBean {
    private String urlContains;//URL包含的字符串
    private String urlKey;
    private Pattern urlRegexPattern;//URL的匹配正则表达式
    private String refContains;//REF包含的字符串
    private String refKey;
    private Pattern refRegexPattern;//REF的匹配正则表达式
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

    public Pattern getUrlRegexPattern() {
        return urlRegexPattern;
    }

    public void setUrlRegexPattern(Pattern urlRegexPattern) {
        this.urlRegexPattern = urlRegexPattern;
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

    public Pattern getRefRegexPattern() {
        return refRegexPattern;
    }

    public void setRefRegexPattern(Pattern refRegexPattern) {
        this.refRegexPattern = refRegexPattern;
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
}
