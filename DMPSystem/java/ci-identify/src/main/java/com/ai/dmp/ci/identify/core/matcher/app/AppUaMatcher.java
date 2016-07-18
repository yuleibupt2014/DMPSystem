package com.ai.dmp.ci.identify.core.matcher.app;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimAppBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleAppBean;
import com.ai.dmp.ci.identify.core.matcher.*;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AppUaMatcher extends AbstractBaseMatcher implements IMatcher {

    private static AppUaMatcher instance = new AppUaMatcher();
    private static Map<String, AppRuleCacheBean> uaMap = new HashMap<String, AppRuleCacheBean>();

    private AppUaMatcher() {
    }

    public static AppUaMatcher getInstance() {
        return instance;
    }

    /**
     * 该方法为根据host和regex匹配上的后续处理过程
     *
     * @return result : true：匹配成功；false:匹配失败
     */
    public boolean match(Result result) throws Exception {
        String uaStr = result.get(CIConst.ResultColName_S.S_UA);
        if (StringUtil.isEmpty(uaStr)) {
            return false;
        }
        List<String> wordList = result.getUaWordList();
        for (String word : wordList) {
            if (uaMap.containsKey(word)) {
                AppRuleCacheBean appCacheBean = uaMap.get(word);
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
                return true;
            }
        }
        return false;
    }

    /**
     * 初始化：加载数据等  AppRuleCacheBean缓存——DimCiRuleAppBean,前者多一个ContId
     *
     * @return
     */
    public void initialize() throws Exception {
        List<DimCiRuleAppBean> appList = MatcherUtil.appRuleList;
        for (DimCiRuleAppBean appBean : appList) {
            if (StringUtil.isEmpty(appBean.getHost()) && !StringUtil.isEmpty(appBean.getUaContains())) {
                AppRuleCacheBean cacheBean = new AppRuleCacheBean();
                cacheBean.setId(appBean.getId());
                cacheBean.setHost(appBean.getHost());
                cacheBean.setUaContains(appBean.getUaContains());
                cacheBean.setAppId(appBean.getAppId());

                //设置app对应的cont_id
                for (DimAppBean dimAppBean : MatcherUtil.dimAppList) {
                    if (cacheBean.getAppId().equals(String.valueOf(dimAppBean.getAppId())) &&
                            !StringUtil.isEmpty(dimAppBean.getContId())) {
                        cacheBean.setContId(dimAppBean.getContId());
                    }
                }
                uaMap.put(appBean.getUaContains(), cacheBean);
            }
        }
    }
}
