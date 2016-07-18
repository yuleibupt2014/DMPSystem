package com.ai.dmp.ci.identify.core.db.bean;

/**
 * Created by yulei on 2015/11/25.
 */
public class DimCiRuleTerminalBean {
    private int id;
    private String terminalFlag;
    private String kw;
    private String regex;
    private String familyReplacement;
    private String v1Replacement;
    private String v2Replacement;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTerminalFlag() {
        return terminalFlag;
    }

    public void setTerminalFlag(String terminalFlag) {
        this.terminalFlag = terminalFlag;
    }

    public String getKw() {
        return kw;
    }

    public void setKw(String kw) {
        this.kw = kw;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
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

    @Override
    public String toString() {
        return "DimCiRuleTerminal{" +
                "id=" + id +
                ", terminalFlag='" + terminalFlag + '\'' +
                ", kw='" + kw + '\'' +
                ", regex='" + regex + '\'' +
                ", familyReplacement='" + familyReplacement + '\'' +
                ", v1Replacement='" + v1Replacement + '\'' +
                ", v2Replacement='" + v2Replacement + '\'' +
                '}';
    }
}
