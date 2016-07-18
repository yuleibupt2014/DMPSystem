package com.ai.dmp.ci.identify.core.blacklist;

import java.util.List;

/**
 * Created by yulei on 2015/10/29.
 */
public class BlackListCacheBean {
    private String blackType;
    private List<String> blackKeyList;

    public String getBlackType() {
        return blackType;
    }

    public void setBlackType(String blackType) {
        this.blackType = blackType;
    }

    public List<String> getBlackKeyList() {
        return blackKeyList;
    }

    public void setBlackKeyList(List<String> blackKeyList) {
        this.blackKeyList = blackKeyList;
    }
}
