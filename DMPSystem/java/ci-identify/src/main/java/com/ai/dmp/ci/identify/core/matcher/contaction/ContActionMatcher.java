package com.ai.dmp.ci.identify.core.matcher.contaction;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.CharsetUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleContActionBean;
import com.ai.dmp.ci.identify.core.matcher.BaseMatcher;
import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContActionMatcher extends BaseMatcher {


    private static ContActionMatcher instance = new ContActionMatcher();

    private ContActionMatcher() {
    }

    public static ContActionMatcher getInstance() {
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
        ContActionRuleCacheBean contActionCacheBean = (ContActionRuleCacheBean) cacheBean;
        //是否应该取值
        boolean isValue = StringUtil.isEmpty(contActionCacheBean.getValueTypeId()) ? false : true;

        String value = null;

        //根据URL获取值
        String url = result.get(CIConst.ResultColName_S.S_URL);
        String urlConntains = contActionCacheBean.getUrlContains();

        //已经被过滤，直接返回
        String filterKey = "lock_" + contActionCacheBean.getValueTypeId();
        if ("-1".equals(result.get(filterKey))) { //过滤
            return FLAG_0;
        }

        if (!StringUtils.isEmpty(urlConntains) && url.indexOf(urlConntains) < 0) {
            return FLAG_0;
        }
        if (isValue && !StringUtil.isEmpty(contActionCacheBean.getUrlKey())) {
            value = result.getUrlParamValue(contActionCacheBean.getUrlKey());
//            if (StringUtil.isEmpty(value)) {
//                return FLAG_0;
//            }
        }
        if (contActionCacheBean.getUrlRegexPattern() != null) {
            Matcher matcher = contActionCacheBean.getUrlRegexPattern().matcher(url);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(value) && isValue) {
                    value = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        //根据Ref获取值
        String ref = result.get(CIConst.ResultColName_S.S_REF);
        String refConntains = contActionCacheBean.getRefContains();
        if (!StringUtils.isEmpty(refConntains) && ref.indexOf(refConntains) < 0) {
            return FLAG_0;
        }
        if (isValue && !StringUtil.isEmpty(contActionCacheBean.getRefKey())) {
            value = result.getRefParamValue(contActionCacheBean.getRefKey());
//            if (StringUtil.isEmpty(value)) {
//                return FLAG_0;
//            }
        }
        if (contActionCacheBean.getRefRegexPattern() != null) {
            Matcher matcher = contActionCacheBean.getRefRegexPattern().matcher(ref);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(value) && isValue) {
                    value = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        //当前规则为过滤规则，设置filterKey=-1
        if ("-1".equals(contActionCacheBean.getUrlKey())) {
            result.set(filterKey, "-1");
            return FLAG_0;
        }

        String valueTypeId = "";
        if (isValue && !StringUtil.isEmpty(value)) {
            if (CIConst.ValueTypeId.KEYWORD_1.equals(contActionCacheBean.getValueTypeId())) {
                value = MatcherUtil.parseKw(url, value);//解析关键字
            }
            if (!StringUtil.isEmpty(value)) {
                value = contActionCacheBean.getPrefix() + StringUtil.filterChar(value, ' ');
                valueTypeId = contActionCacheBean.getValueTypeId();
            }
        }

        ContActionEntry entry = new ContActionEntry(
                contActionCacheBean.getContId(),
                contActionCacheBean.getActionId(),
                valueTypeId,
                value,
                String.valueOf(contActionCacheBean.getId()),
                null);
        result.addContAction(entry);

        return FLAG_4;
    }

    /**
     * 将数据库对象转换为缓存对象,如果该方法里面验证未通过，则返回null
     */
    protected BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception {
        DimCiRuleContActionBean contActionBean = (DimCiRuleContActionBean) bean;
        try {
            ContActionRuleCacheBean cacheBean = new ContActionRuleCacheBean();
            cacheBean.setId(contActionBean.getId());
            cacheBean.setHost(contActionBean.getHost() == null ? "" : contActionBean.getHost().trim());
            cacheBean.setUrlContains(contActionBean.getUrlContains());
            cacheBean.setUrlKey(contActionBean.getUrlKey());
            cacheBean.setRefContains(contActionBean.getRefContains());
            cacheBean.setRefKey(contActionBean.getRefKey());
            cacheBean.setContId(contActionBean.getContId());
            cacheBean.setActionId(contActionBean.getActionId());
            cacheBean.setValueTypeId(contActionBean.getValueTypeId());
            cacheBean.setPrefix(contActionBean.getPrefix() == null ? "" : contActionBean.getPrefix());

            if (!StringUtils.isEmpty(contActionBean.getUrlRegex())) {
                cacheBean.setUrlRegexPattern(Pattern.compile(contActionBean.getUrlRegex(),
                        Pattern.CASE_INSENSITIVE));
            }

            if (!StringUtils.isEmpty(contActionBean.getRefRegex())) {
                cacheBean.setRefRegexPattern(Pattern.compile(contActionBean.getRefRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            return cacheBean;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info(contActionBean.toString());
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
        super.matchList = MatcherUtil.contActionRuleList;

        //host是否左模糊匹配
        isLeftLike = Config.getBoolean(CIConst.Config.IS_CONT_ACTION_MATCHER_HOST_LEFT_LIKE);
    }

}
