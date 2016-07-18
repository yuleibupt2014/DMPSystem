package com.ai.dmp.ci.identify.core.matcher.userflag;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleLocBean;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleUserFlagBean;
import com.ai.dmp.ci.identify.core.matcher.BaseMatcher;
import com.ai.dmp.ci.identify.core.matcher.BaseRuleCacheBean;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import com.ai.dmp.ci.identify.core.matcher.loc.LocRuleCacheBean;
import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserFlagMatcher extends BaseMatcher {

    private static final Pattern macPattern = Pattern.compile("^[0-9a-fA-F]{12}$|^[0-9a-fA-F:_@#-]{17}$");
    private static final Pattern imeiImsiPattern = Pattern.compile("^\\d{15}$");
    public static final Pattern idfaPattern = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern phoneNoPattern = Pattern.compile("^1(\\d){10}$");
    private static final Pattern emailPattern = Pattern.compile("^[a-z0-9]+([._\\\\-]*[a-z0-9])*@([a-z0-9]+[-a-z0-9]*[a-z0-9]+.){1,63}[a-z0-9]+$");

    private static UserFlagMatcher instance = new UserFlagMatcher();

    private UserFlagMatcher() {
    }

    public static UserFlagMatcher getInstance() {
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
        UserFlagRuleCacheBean userFlagCacheBean = (UserFlagRuleCacheBean) cacheBean;

        //已经存在结果，userFlagCacheBean.getFlagType()类型不再进行匹配
        if(!StringUtil.isEmpty(result.get(userFlagCacheBean.getFlagType()))){
            return FLAG_0;
        }

        String value = null;
        if (!StringUtil.isEmpty(userFlagCacheBean.getUrlKey())) {
            value = result.getUrlParamValue(userFlagCacheBean.getUrlKey());
        }
        String url = result.get(CIConst.ResultColName_S.S_URL);
        if (userFlagCacheBean.getUrlRegexPattern() != null) {
            Matcher matcher = userFlagCacheBean.getUrlRegexPattern().matcher(url);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(value)) {
                    value = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        if (StringUtil.isEmpty(value) && !StringUtil.isEmpty(userFlagCacheBean.getCookieKey())) {
            value = result.getCookieParamValue(userFlagCacheBean.getCookieKey());
        }
        String cookie = result.get(CIConst.ResultColName_S.S_COOKIE);
        if (userFlagCacheBean.getCookieRegexPattern() != null) {
            Matcher matcher = userFlagCacheBean.getCookieRegexPattern().matcher(cookie);
            if (matcher.find()) {
                if (matcher.groupCount() >= 1 && StringUtil.isEmpty(value)) {
                    value = matcher.group(1);
                }
            } else {
                return FLAG_0;
            }
        }

        if (!StringUtil.isEmpty(value)) {
            String userFlag = userFlagCacheBean.getFlagType();
            if (CIConst.UserFlagType.MAC.equals(userFlag)) {
                if (value.indexOf("%") >= 0) {
                    value = URLDecoder.decode(value, "utf8");
                }
                if (value.indexOf("%") >= 0) {
                    value = URLDecoder.decode(value, "utf8");
                }
                if (!macPattern.matcher(value).find()) {
                    return FLAG_0;
                }
            }
            else if (CIConst.UserFlagType.PHONE_NO.equals(userFlag)) {
                if (!phoneNoPattern.matcher(value).find()) {
                    return FLAG_0;
                }
            } else if (CIConst.UserFlagType.IMEI.equals(userFlag) || CIConst.UserFlagType.IMSI.equals(userFlag)) {
                if (!imeiImsiPattern.matcher(value).find()) {
                    return FLAG_0;
                }
            } else if (CIConst.UserFlagType.IDFA.equals(userFlag)) {
                if (!idfaPattern.matcher(value).find()) {
                    return FLAG_0;
                }
            } else if (CIConst.UserFlagType.EMAIL.equals(userFlag)) {
                if (!emailPattern.matcher(value).find()) {
                    return FLAG_0;
                }
            } else if (CIConst.UserFlagType.USER_NAME.equals(userFlag)) {
                value = MatcherUtil.parseKw(url, value);
            }
            value = userFlagCacheBean.getPrefix() + value;
            result.set(userFlag, value);
            result.set(userFlag + "_rule_id", String.valueOf(userFlagCacheBean.getId()));
            return FLAG_4;
        } else {
            return FLAG_0;
        }
    }

    /**
     * 将数据库对象转换为缓存对象,如果该方法里面验证未通过，则返回null
     */
    protected BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception {
        DimCiRuleUserFlagBean userFlagBean = (DimCiRuleUserFlagBean) bean;
        try {
            UserFlagRuleCacheBean cacheBean = new UserFlagRuleCacheBean();
            cacheBean.setId(userFlagBean.getId());
            cacheBean.setHost(userFlagBean.getHost() == null ? "" : userFlagBean.getHost().trim());
            cacheBean.setFlagType(userFlagBean.getFlagType());
            cacheBean.setUrlKey(userFlagBean.getUrlKey());
            cacheBean.setCookieKey(userFlagBean.getCookieKey());
            if (!StringUtils.isEmpty(userFlagBean.getUrlRegex())) {
                cacheBean.setUrlRegexPattern(Pattern.compile(userFlagBean.getUrlRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            if (!StringUtils.isEmpty(userFlagBean.getCookieRegex())) {
                cacheBean.setCookieRegexPattern(Pattern.compile(userFlagBean.getCookieRegex(),
                        Pattern.CASE_INSENSITIVE));
            }
            cacheBean.setPrefix(userFlagBean.getPrefix() == null ? "" : userFlagBean.getPrefix());
            return cacheBean;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info(userFlagBean.toString());
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
        super.matchList = MatcherUtil.userFlagList;

        //host是否左模糊匹配
        isLeftLike = Config.getBoolean(CIConst.Config.IS_USER_FLAG_MATCHER_HOST_LEFT_LIKE);
    }

}
