package com.ai.dmp.ci.identify.core.matcher.loc;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleLocBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleSiteBean;
import com.ai.dmp.ci.identify.core.matcher.BaseMatcher;
import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import com.ai.dmp.ci.identify.core.matcher.site.SiteRuleCacheBean;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocMatcher extends BaseMatcher {

    private static LocMatcher instance = new LocMatcher();

    private LocMatcher() {
    }

    public static LocMatcher getInstance() {
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
        LocRuleCacheBean locCacheBean = (LocRuleCacheBean) cacheBean;
        String lng = null;//经度
        String lat = null;//纬度

        if (!StringUtil.isEmpty(locCacheBean.getLngKey())) {
            lng = result.getUrlParamValue(locCacheBean.getLngKey());
        }
        if (!StringUtil.isEmpty(locCacheBean.getLatKey())) {
            lat = result.getUrlParamValue(locCacheBean.getLatKey());
        }

        String url = result.get(CIConst.ResultColName_S.S_URL);
        if (locCacheBean.getLngRegexPattern() != null) {
            Matcher matcher = locCacheBean.getLngRegexPattern().matcher(url);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(lng)) {
                    lng = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        if (locCacheBean.getLatRegexPattern() != null) {
            Matcher matcher = locCacheBean.getLatRegexPattern().matcher(url);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(lat)) {
                    lat = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        if (!StringUtil.isEmpty(lng) && !StringUtil.isEmpty(lat)) {
            String loc = locCacheBean.getPrefix() + lng + "," + lat;
            result.set(CIConst.ResultColName.LOC_ID, loc);
            result.set(CIConst.ResultColName_RuleID.LOC_ID_RULE_ID, String.valueOf(locCacheBean.getId()));
            return FLAG_1;
        } else {
            return FLAG_0;
        }
    }

    /**
     * 将数据库对象转换为缓存对象,如果该方法里面验证未通过，则返回null
     */
    protected BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception {
        DimCiRuleLocBean locBean = (DimCiRuleLocBean) bean;
        try {
            LocRuleCacheBean cacheBean = new LocRuleCacheBean();
            cacheBean.setId(locBean.getId());
            cacheBean.setHost(locBean.getHost() == null ? "" : locBean.getHost().trim());
            cacheBean.setLatKey(locBean.getLatKey());
            cacheBean.setLngKey(locBean.getLngKey());
            cacheBean.setPrefix(locBean.getPrefix() == null ? "" : locBean.getPrefix());
            if (!StringUtils.isEmpty(locBean.getLatRegex())) {
                cacheBean.setLatRegexPattern(Pattern.compile(locBean.getLatRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            if (!StringUtils.isEmpty(locBean.getLngRegex())) {
                cacheBean.setLngRegexPattern(Pattern.compile(locBean.getLngRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            return cacheBean;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info(locBean.toString());
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
        super.matchList = MatcherUtil.locRuleList;

        //host是否左模糊匹配
        isLeftLike = Config.getBoolean(CIConst.Config.IS_LOC_MATCHER_HOST_LEFT_LIKE);
    }

}
