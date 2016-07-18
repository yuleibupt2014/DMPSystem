package com.ai.dmp.ci.identify.core.matcher.site;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleAppBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleSiteBean;
import com.ai.dmp.ci.identify.core.matcher.BaseMatcher;
import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SiteMatcher extends BaseMatcher {

    private static SiteMatcher instance = new SiteMatcher();

    private SiteMatcher() { }

    public static SiteMatcher getInstance() {
        return instance;
    }

    /**
     * 该方法为根据host和regex匹配上的后续处理过程
     *
     * @param result
     * @param cacheBean
     * @return result 0:匹配失败，继续匹配
     * 1:匹配成功，返回。
     * 2:过滤匹配成功，返回
     * 3:匹配成功，继续匹配，但host不继续左模糊
     */
    protected int handleMatch(BaseRuleCacheBean cacheBean, Result result) throws Exception {
        SiteRuleCacheBean siteCacheBean = (SiteRuleCacheBean) cacheBean;
        result.set(CIConst.ResultColName.SITE_ID,String.valueOf(siteCacheBean.getSiteId()));
        result.set(CIConst.ResultColName_RuleID.SITE_ID_RULE_ID,String.valueOf(siteCacheBean.getId()));
        return FLAG_1;
    }

    /**
     * 将数据库对象转换为缓存对象,如果该方法里面验证未通过，则返回null
     */
    protected BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception {
        DimCiRuleSiteBean siteBean = (DimCiRuleSiteBean) bean;
        try {
            SiteRuleCacheBean cacheBean = new SiteRuleCacheBean();
            cacheBean.setId(siteBean.getId());
            cacheBean.setHost(siteBean.getHost() == null ? "" : siteBean.getHost().trim());
            cacheBean.setSiteId(siteBean.getSiteId());
            return cacheBean;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info(siteBean.toString());
        }
        return null;
    }

    /**
     * 初始化：加载数据等
     *
     * @return
     */
    @Override
    protected void init() throws Exception {
        super.matchList = MatcherUtil.siteRuleList;

        //host是否左模糊匹配
        isLeftLike = Config.getBoolean(CIConst.Config.IS_SITE_MATCHER_HOST_LEFT_LIKE);
    }

}
