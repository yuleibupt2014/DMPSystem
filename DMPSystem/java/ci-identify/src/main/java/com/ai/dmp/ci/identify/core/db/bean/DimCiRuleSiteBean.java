package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleSiteBean extends BaseMatchBean{
    private String siteId;//站点ID

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    @Override
    public String toString() {
        return "DimCiRuleSiteBean{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", siteId='" + siteId + '\'' +
                '}';
    }
}
