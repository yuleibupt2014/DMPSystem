package com.ai.dmp.ci.identify.core.matcher.terminal;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/11/25.
 */
public class TerminalRuleCacheBean {
    private int id;
    private String terminalFlag;
    private Pattern regexPattern;
    private String familyReplacement;
    private String v1Replacement;
    private String v2Replacement;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Pattern getRegexPattern() {
        return regexPattern;
    }

    public void setRegexPattern(Pattern regexPattern) {
        this.regexPattern = regexPattern;
    }

    public String getFamilyReplacement() {
        return familyReplacement;
    }

    public void setFamilyReplacement(String familyReplacement) {
        this.familyReplacement = familyReplacement;
    }

    public String getV1Replacement() {
        return v1Replacement;
    }

    public void setV1Replacement(String v1Replacement) {
        this.v1Replacement = v1Replacement;
    }

    public String getV2Replacement() {
        return v2Replacement;
    }

    public void setV2Replacement(String v2Replacement) {
        this.v2Replacement = v2Replacement;
    }

    public String getTerminalFlag() {
        return terminalFlag;
    }

    public void setTerminalFlag(String terminalFlag) {
        this.terminalFlag = terminalFlag;
    }

    public TerminalRuleCacheBean(int id, String terminalFlag,Pattern regexPattern, String familyReplacement, String v1Replacement, String v2Replacement) {
        this.id = id;
        this.terminalFlag = terminalFlag;
        this.regexPattern = regexPattern;
        this.familyReplacement = familyReplacement;
        this.v1Replacement = v1Replacement;
        this.v2Replacement = v2Replacement;
    }
}
