package com.ai.dmp.ci.identify.core.db.bean;

public class DimCiRuleLocBean extends BaseMatchBean {
    private String lngKey; //经度在URL中的参数
    private String latKey; //纬度在URL中的参数
    private String lngRegex;  //经度在URL中的正则
    private String latRegex; //纬度在URL中的正则
    private String prefix; //前缀   如：baidu-    因为百度获取的值只是baidu的id，而不是经纬度，所有需要后续模块统一处理。

    public String getLngKey() {
        return lngKey;
    }

    public void setLngKey(String lngKey) {
        this.lngKey = lngKey;
    }

    public String getLatKey() {
        return latKey;
    }

    public void setLatKey(String latKey) {
        this.latKey = latKey;
    }

    public String getLngRegex() {
        return lngRegex;
    }

    public void setLngRegex(String lngRegex) {
        this.lngRegex = lngRegex;
    }

    public String getLatRegex() {
        return latRegex;
    }

    public void setLatRegex(String latRegex) {
        this.latRegex = latRegex;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "DimCiRuleLocBean{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", lngKey='" + lngKey + '\'' +
                ", latKey='" + latKey + '\'' +
                ", lngRegex='" + lngRegex + '\'' +
                ", latRegex='" + latRegex + '\'' +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
