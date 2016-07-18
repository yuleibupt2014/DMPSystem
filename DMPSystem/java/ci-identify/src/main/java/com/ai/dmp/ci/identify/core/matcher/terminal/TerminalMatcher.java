package com.ai.dmp.ci.identify.core.matcher.terminal;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleTerminalBean;
import com.ai.dmp.ci.identify.core.db.bean.DimUaKwBean;
import com.ai.dmp.ci.identify.core.db.dao.DaoUtil;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/11/25.
 */
public class TerminalMatcher {
    protected static Logger log = Logger.getLogger(TerminalMatcher.class);

    private Map<String, Integer> uaKwPriorityMap = new HashMap<String, Integer>();

    private String[] terminals = new String[]{CIConst.ResultColName.DEVICE_MODEL, CIConst.ResultColName.DEVICE_TYPE, CIConst.ResultColName.DEVICE_OS, CIConst.ResultColName.DEVICE_BROWSER};

    //格式：<terminalFlag,<kw,List<TerminalRuleCacheBean>>>
    private Map<String, Map<String, List<TerminalRuleCacheBean>>> configMap = new HashMap<String, Map<String, List<TerminalRuleCacheBean>>>();

    //缓存UA与型号、类型、操作系统、浏览器的关系
    private Map<String, Terminal> CACHE = new LRUMap(Config.getInt("ua.cache.count"));

    public boolean match(Result result) throws Exception {
        String sUa = result.get(CIConst.ResultColName_S.S_UA);
        if (StringUtil.isEmpty(sUa)) {
            return false;
        }

        Terminal terminal = CACHE.get(sUa);
        if (terminal != null) {
            result.set(CIConst.ResultColName.DEVICE_MODEL, terminal.model);
            result.set(CIConst.ResultColName_RuleID.DEVICE_MODEL_RULE_ID, terminal.modelRuleId);
            result.set(CIConst.ResultColName.DEVICE_TYPE, terminal.type);
            result.set(CIConst.ResultColName_RuleID.DEVICE_TYPE_RULE_ID, terminal.typeRuleId);
            result.set(CIConst.ResultColName.DEVICE_OS, terminal.os);
            result.set(CIConst.ResultColName_RuleID.DEVICE_OS_RULE_ID, terminal.osRuleId);
            result.set(CIConst.ResultColName.DEVICE_BROWSER, terminal.browser);
            result.set(CIConst.ResultColName_RuleID.DEVICE_BROWSER_RULE_ID, terminal.browserRuleId);
            return true;
        }

        List<String> wordList = sort(result.getUaWordList(), uaKwPriorityMap);//获取UA分词列表
        result.setUaWordList(wordList); //放回result对象中，以便在appUaMatcher时使用已经排序过的列表

        boolean flag = false;
        outer:
        for (String terminalFlag : terminals) {
            for (String word : wordList) {
                flag = parse(result, sUa, word, terminalFlag);
                if (flag) {
                    continue outer;
                }
            }
        }

        //添加缓存
        terminal = new Terminal(
                result.get(CIConst.ResultColName.DEVICE_MODEL), result.get(CIConst.ResultColName_RuleID.DEVICE_MODEL_RULE_ID),
                result.get(CIConst.ResultColName.DEVICE_TYPE), result.get(CIConst.ResultColName_RuleID.DEVICE_TYPE_RULE_ID),
                result.get(CIConst.ResultColName.DEVICE_OS), result.get(CIConst.ResultColName_RuleID.DEVICE_OS_RULE_ID),
                result.get(CIConst.ResultColName.DEVICE_BROWSER), result.get(CIConst.ResultColName_RuleID.DEVICE_BROWSER_RULE_ID));
        CACHE.put(sUa, terminal);

        return true;
    }

    private boolean parse(Result result, String ua, String word, String terminalFlag) {
        if (!StringUtil.isEmpty(result.get(terminalFlag))) { //如果已经解析过，则不再解析
            return false;
        }
        Map<String, List<TerminalRuleCacheBean>> map = configMap.get(terminalFlag);
        if (map == null || map.size() == 0) {
            return false;
        }

        List<TerminalRuleCacheBean> cacheList = map.get(word);
        if (cacheList == null || cacheList.size() == 0) {
            return false;
        }

        for (TerminalRuleCacheBean bean : cacheList) {
            String value = bean.getFamilyReplacement();

            if (bean.getRegexPattern() != null) {
                value = getValue(bean, ua, terminalFlag);
            }

            if (StringUtil.isEmpty(value)) {
                continue;
            }

            result.set(terminalFlag, value);
            result.set(terminalFlag + "_rule_id", String.valueOf(bean.getId()));
            return true;
        }
        return false;
    }

    private String getValue(TerminalRuleCacheBean bean, String ua, String terminalFlag) {
        Matcher matcher = bean.getRegexPattern().matcher(ua);
        if (!matcher.find()) {
            return null;
        }

        int groupCount = matcher.groupCount();
        String family = bean.getFamilyReplacement();
        //解析family
        if (groupCount >= 1) {
            if (!StringUtils.isEmpty(family)) {
                if (family.contains("$1")) {
                    family = family.replaceAll("\\$1", matcher.group(1));
                }
            } else {
                family = matcher.group(1);
            }
        }

        if (StringUtil.isEmpty(family)) {
            return null;
        }

        String value = family;
        //解析v1
        String v1Replacement = bean.getV1Replacement();
        String v1 = "";
        if (!StringUtil.isEmpty(v1Replacement)) {
            v1 = v1Replacement;
        } else if (groupCount >= 2) {
            v1 = matcher.group(2);
        }

        //解析v2
        String v2Replacement = bean.getV1Replacement();
        String v2 = "";
        if (!StringUtil.isEmpty(v2Replacement)) {
            v2 = v2Replacement;
        } else if (groupCount >= 3) {
            v2 = matcher.group(3);
        }

        if (!StringUtil.isEmpty(v1)) {
            value = value + " " + v1;
        }
        if (!StringUtil.isEmpty(v2)) {
            value = value + "." + v2;
        }

        return value;
    }

    /**
     * 将srcKwList的元素根据priorityMap优先级进行排序（priorityMap不存在的元素，排在最后面）
     *
     * @param srcKwList
     * @param priorityMap
     * @return
     */
    public List<String> sort(List<String> srcKwList, Map<String, Integer> priorityMap) {
        int priority;
        Integer priorityObj;
        KwPriority kwp;
        KwPriority[] kwpArray = new KwPriority[srcKwList.size()];
        int i = 0;
        for (String kw : srcKwList) {
            priorityObj = priorityMap.get(kw);
            if (priorityObj == null) {
                priority = Integer.MAX_VALUE;
            } else {
                priority = priorityObj;
            }
            kwp = new KwPriority(kw, priority);
            kwpArray[i] = kwp;
            i++;
        }

        Arrays.sort(kwpArray);    //排序

        List<String> sortedList = new ArrayList<String>(kwpArray.length);
        for (KwPriority kw : kwpArray) {
            sortedList.add(kw.kw);
        }
        return sortedList;
    }

    class KwPriority implements java.lang.Comparable {
        String kw;
        int priority;

        public KwPriority(String kw, int priority) {
            this.kw = kw;
            this.priority = priority == 0 ? Integer.MAX_VALUE : priority;
        }

        @Override
        public int compareTo(Object o) {
            KwPriority kw = (KwPriority) o;
            return this.priority - kw.priority;
        }
    }

    public void initialize() throws Exception {
        List<DimCiRuleTerminalBean> list = MatcherUtil.terminalList;

        for (DimCiRuleTerminalBean bean : list) {
            if (StringUtil.isEmpty(bean.getKw()) || StringUtil.isEmpty(bean.getTerminalFlag())) {
                log.warn("kw,terminal_flag存在为空！过滤该规则！" + bean);
                continue;
            }

            if (StringUtil.isEmpty(bean.getFamilyReplacement()) && StringUtil.isEmpty(bean.getRegex())) {
                log.warn("familyReplacement和regex不能同时为空！过滤该规则！" + bean);
                continue;
            }

            String terminalFlag = bean.getTerminalFlag();
            Map<String, List<TerminalRuleCacheBean>> kwMap = configMap.get(terminalFlag);
            if (kwMap == null) {
                kwMap = new HashMap<String, List<TerminalRuleCacheBean>>();
                configMap.put(terminalFlag, kwMap);
            }

            String kw = bean.getKw();
            List<TerminalRuleCacheBean> beanList = kwMap.get(kw);
            if (beanList == null) {
                beanList = new ArrayList<TerminalRuleCacheBean>();
                kwMap.put(kw, beanList);
            }

            TerminalRuleCacheBean terminalCacheBean =
                    new TerminalRuleCacheBean(bean.getId(),
                            bean.getTerminalFlag(),
                            StringUtil.isEmpty(bean.getRegex()) ? null : Pattern.compile(bean.getRegex()),
                            bean.getFamilyReplacement(),
                            bean.getV1Replacement(),
                            bean.getV2Replacement()
                    );
            beanList.add(terminalCacheBean);
        }

        //缓存ua相关关键词的优先级
        List<DimUaKwBean> uaKwList = MatcherUtil.uaKwList;
        for (DimUaKwBean uaKwBean : uaKwList) {
            if (StringUtil.isEmpty(uaKwBean.getKw())) {
                continue;
            }
            uaKwPriorityMap.put(uaKwBean.getKw(), uaKwBean.getPriority());
        }
    }

    class Terminal {
        public String model;
        public String modelRuleId;
        public String type;
        public String typeRuleId;
        public String os;
        public String osRuleId;
        public String browser;
        public String browserRuleId;

        public Terminal(String model, String modelRuleId,
                        String type, String typeRuleId,
                        String os, String osRuleId,
                        String browser, String browserRuleId) {
            this.model = model;
            this.modelRuleId = modelRuleId;
            this.type = type;
            this.typeRuleId = typeRuleId;
            this.os = os;
            this.osRuleId = osRuleId;
            this.browser = browser;
            this.browserRuleId = browserRuleId;
        }
    }

}
