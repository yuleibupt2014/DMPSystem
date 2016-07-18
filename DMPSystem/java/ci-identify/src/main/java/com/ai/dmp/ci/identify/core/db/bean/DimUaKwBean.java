package com.ai.dmp.ci.identify.core.db.bean;

public class DimUaKwBean {
    private int id;
    private String kw;
    private int priority;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKw() {
        return kw;
    }

    public void setKw(String kw) {
        this.kw = kw;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return "DimUaKwBean{" +
                "id='" + id + '\'' +
                ", kw='" + kw + '\'' +
                ", priority='" + priority + '\'' +
                '}';
    }
}
