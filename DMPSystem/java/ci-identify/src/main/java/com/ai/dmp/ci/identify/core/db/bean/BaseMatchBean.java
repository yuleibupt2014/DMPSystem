package com.ai.dmp.ci.identify.core.db.bean;

public class BaseMatchBean {
    protected int id;//规则ID
    protected String host;//host

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
