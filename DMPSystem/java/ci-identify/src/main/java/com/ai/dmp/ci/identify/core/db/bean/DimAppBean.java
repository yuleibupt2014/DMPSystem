package com.ai.dmp.ci.identify.core.db.bean;

public class DimAppBean {
    private int appId;
    private String contId;

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public String getContId() {
        return contId;
    }

    public void setContId(String contId) {
        this.contId = contId;
    }

    @Override
    public String toString() {
        return "DimAppBean{" +
                "appId=" + appId +
                ", contId='" + contId + '\'' +
                '}';
    }
}
