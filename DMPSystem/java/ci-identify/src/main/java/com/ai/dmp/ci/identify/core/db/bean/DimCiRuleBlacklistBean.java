package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleBlacklistBean {
    private int id;
    private String blackType;
    private String blackKey;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBlackType() {
        return blackType;
    }

    public void setBlackType(String blackType) {
        this.blackType = blackType;
    }

    public String getBlackKey() {
        return blackKey;
    }

    public void setBlackKey(String blackKey) {
        this.blackKey = blackKey;
    }

    @Override
    public String toString() {
        return "DimCiRuleBlacklistBean{" +
                "id=" + id +
                ", blackType='" + blackType + '\'' +
                ", blackKey='" + blackKey + '\'' +
                '}';
    }
}
