package com.ai.dmp.ci.identify.core.matcher.site;

import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;

import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/20.
 */
public class SiteRuleCacheBean extends BaseRuleCacheBean {
    private String siteId;

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }
}
