package com.ai.dmp.ci.identify.core.matcher.app;

import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;

import com.ai.dmp.ci.identify.core.db.bean.DimAppBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleAppBean;
import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;
import com.ai.dmp.ci.identify.core.matcher.BaseMatcher;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AppMatcher extends BaseMatcher {

    private static AppMatcher instance = new AppMatcher();

    private AppMatcher() {
    }

    public static AppMatcher getInstance() {
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
        AppRuleCacheBean appCacheBean = (AppRuleCacheBean) cacheBean;
        String url = result.get(CIConst.ResultColName_S.S_URL);
        String urlConntains = appCacheBean.getUrlContains();
        if (!StringUtils.isEmpty(urlConntains) && url.indexOf(urlConntains) < 0) {
            return FLAG_0;
        }

        String ua = result.get(CIConst.ResultColName_S.S_UA);
        String uaConntains = appCacheBean.getUaContains();
        if (!StringUtils.isEmpty(uaConntains) && ua.indexOf(uaConntains) < 0) {
            return FLAG_0;
        }

        if (appCacheBean.getUrlRegexPattern() != null) {
            Matcher matcher = appCacheBean.getUrlRegexPattern().matcher(url);
            if (!matcher.find()) {
                return FLAG_0;
            }
        }

        if (appCacheBean.getUaRegexPattern() != null) {
            Matcher matcher = appCacheBean.getUaRegexPattern().matcher(ua);
            if (!matcher.find()) {
                return FLAG_0;
            }
        }

        result.set(CIConst.ResultColName.APP_ID, String.valueOf(appCacheBean.getAppId()));
        result.set(CIConst.ResultColName_RuleID.APP_ID_RULE_ID, String.valueOf(appCacheBean.getId()));

        //根据app打内容标签
        if (result.getContActionList().size() == 0 && !StringUtil.isEmpty(appCacheBean.getContId())) {
            ContActionEntry entry = new ContActionEntry(
                    appCacheBean.getContId(),
                    null,
                    null,
                    null,
                    null,
                    String.valueOf(appCacheBean.getId()));
            result.addContAction(entry);
        }

        return FLAG_1;
    }

    /**
     * 将数据库对象转换为缓存对象,如果该方法里面验证未通过，则返回null
     */
    protected BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception {
        DimCiRuleAppBean appBean = (DimCiRuleAppBean) bean;
        try {
            AppRuleCacheBean cacheBean = new AppRuleCacheBean();
            if (StringUtil.isEmpty(appBean.getHost())) {
                return null;
            }
            cacheBean.setId(appBean.getId());
            cacheBean.setHost(appBean.getHost() == null ? "" : appBean.getHost().trim());
            cacheBean.setUrlContains(appBean.getUrlContains());
            if (!StringUtils.isEmpty(appBean.getUrlRegex())) {
                cacheBean.setUrlRegexPattern(Pattern.compile(appBean.getUrlRegex(),
                        Pattern.CASE_INSENSITIVE));
            }

            cacheBean.setUaContains(appBean.getUaContains());
            if (!StringUtils.isEmpty(appBean.getUaRegex())) {
                cacheBean.setUaRegexPattern(Pattern.compile(appBean.getUaRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            cacheBean.setAppId(appBean.getAppId());

            //设置app对应的cont_id
            for (DimAppBean dimAppBean : MatcherUtil.dimAppList) {
                if (cacheBean.getAppId().equals(String.valueOf(dimAppBean.getAppId())) &&
                        !StringUtil.isEmpty(dimAppBean.getContId())) {
                    cacheBean.setContId(dimAppBean.getContId());
                }
            }

            return cacheBean;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info(appBean.toString());
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
        super.matchList = MatcherUtil.appRuleList;

        //host是否左模糊匹配
        isLeftLike = Config.getBoolean(CIConst.Config.IS_APP_MATCHER_HOST_LEFT_LIKE);
    }
}
