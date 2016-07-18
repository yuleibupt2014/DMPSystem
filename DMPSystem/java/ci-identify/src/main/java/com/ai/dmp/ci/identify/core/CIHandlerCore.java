package com.ai.dmp.ci.identify.core;

import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.core.blacklist.BlackListFilter;
import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.UrlUtil;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.core.matcher.app.AppUaMatcher;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionMatcher;
import com.ai.dmp.ci.identify.core.matcher.loc.LocMatcher;
import com.ai.dmp.ci.identify.core.matcher.site.SiteMatcher;
import com.ai.dmp.ci.identify.core.matcher.terminal.TerminalMatcher;
import com.ai.dmp.ci.identify.core.matcher.userflag.UserFlagMatcher;
import com.ai.dmp.ci.identify.dataclean.DataCleaning;
import com.ai.dmp.ci.identify.core.matcher.IMatcher;
import com.ai.dmp.ci.identify.core.matcher.app.AppMatcher;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import eu.bitwalker.useragentutils.Version;

import org.apache.log4j.Logger;

public class CIHandlerCore {
    private static Logger log = Logger.getLogger(CIHandlerCore.class);

    private static IMatcher appUaMatcher;//根据UA识别应用的匹配类
    private static IMatcher appMatcher;//应用识别匹配类
    private static IMatcher siteMatcher;//站点识别匹配类
    private static IMatcher contActionMatcher;//内容行为识别匹配类
    private static IMatcher locMatcher;//经纬度识别匹配类
    private static IMatcher userFlagMatcher;//用户标志识别匹配类

    private static TerminalMatcher terminalMatcher; //终端识别匹配类

    private static BlackListFilter blackListFilter = null;//黑名单过滤

    private AbstractAdapter adapter;//项目地适配器
    private static DataCleaning dc; //数据清洗

    static {
        init();//初始化方法：主要加载商品识别规则、应用识别规则、内容识别规则、终端识别规则等
    }

    public CIHandlerCore(AbstractAdapter adapter) {  //在map的setup阶段执行
        this.adapter = adapter;
        dc = new DataCleaning(adapter);//创建数据清洗类
    }

    /**
     * @param value
     * @return
     * @throws Exception
     */
    public Result execute(String value) throws Exception {   //Result里面的 valueMap存有输入的原始数据map
        Result result = dc.cleaning(value); //判断URL是否合法 //判断电话号码是否为空  如果电话号码为空，需要清洗掉

        if (result.ifCleaned()) { //如果有问题，则执行该句弹出去
            return result;
        }

        //适配器的预处理
        adapter.preHandleMatcher(result);

        //基础解析：解析顶级域名和完整域名
        parse(result);

        //用户标志识别：包括cookie_id/user_name/email/phone_no/imei/imsi/mac/idfa/android_id等
        userFlagMatcher.match(result);

        //终端识别, 特别说明： terminalMatcher最好在appUaMatcher之前
        terminalMatcher.match(result);

        //站点识别
        siteMatcher.match(result);

        //行为内容识别 , 特别说明：contActionMatcher必须在appUaMatcher和appMatcher之前
        contActionMatcher.match(result);

        //位置识别
        locMatcher.match(result);

        //APP_ID识别
        boolean flag = appUaMatcher.match(result); //先根据UA进行识别，在根据host进行识别
        if (!flag) { //如果没有通过UA识别，则通过host识别
            appMatcher.match(result);
        }

        //黑名单过滤不合法的值
        blackListFilter.filter(result);

        return result;
    }

    /**
     * 基础解析：解析顶级域名和完整域名
     *
     * @param result
     * @throws Exception
     */
    private void parse(Result result) throws Exception {
        //解析顶级域名和完整域名
        String topDomain = null;//顶级域名
        String compDomain = UrlUtil.getHost(result.get(CIConst.ResultColName_S.S_URL));//完整域名
        if (UrlUtil.isIP(compDomain)) {
            topDomain = compDomain;
            result.set(CIConst.ResultColName.TOP, topDomain);
            result.set(CIConst.ResultColName.TOP_DOMAIN_EXCEPT_TOP, topDomain);
        } else {
            String top = UrlUtil.levelDomain(compDomain, 0).getKey();//返回com.cn、com、cn、org等
            result.set(CIConst.ResultColName.TOP, top);
            topDomain = UrlUtil.levelDomainValue(compDomain, 1);
            result.set(CIConst.ResultColName.TOP_DOMAIN_EXCEPT_TOP, topDomain);
            if (top != null && !"".equals(top) && topDomain != null && !"".equals(topDomain)) {
                topDomain = topDomain + "." + top;
            }
        }
        result.set(CIConst.ResultColName.TOP_DOMAIN, topDomain);
        result.set(CIConst.ResultColName.COMP_DOMAIN, compDomain);

        //解析LAC_CI
        result.set(CIConst.ResultColName.LAC, result.get(CIConst.ResultColName_S.S_LAC));
        result.set(CIConst.ResultColName.CI, result.get(CIConst.ResultColName_S.S_CI));

        //解析UA
//        String ua = result.get(CIConst.ResultColName_S.S_UA);
//        if (!StringUtil.isEmpty(ua)) {
//            UserAgent userAgent = new UserAgent(ua);
//            OperatingSystem os = userAgent.getOperatingSystem();
//            Browser browser = userAgent.getBrowser();
//            Version bv = userAgent.getBrowserVersion();
//            result.set(CIConst.ResultColName.UA_UNIFY, os + "#" + browser + " " + bv);
//        }
    }

    /**
     * 初始化方法：主要加载商品识别规则、应用识别规则、内容识别规则、终端识别规则等
     */
    private static void init() {   //appUaMatcher 里面有 Map<String, AppRuleCacheBean> uaMap以及match等方法，
        appUaMatcher = AppUaMatcher.getInstance();   //根据UA识别应用的匹配类
        appMatcher = AppMatcher.getInstance();    //应用识别匹配类
        siteMatcher = SiteMatcher.getInstance();   //站点识别匹配类
        contActionMatcher = ContActionMatcher.getInstance();  //内容行为识别匹配类
        locMatcher = LocMatcher.getInstance();   //经纬度识别匹配类
        userFlagMatcher = UserFlagMatcher.getInstance();  //用户标志识别匹配类

        blackListFilter = BlackListFilter.getInstance();   //黑名单过滤
        terminalMatcher = new TerminalMatcher();   //终端识别匹配类

        try {//初始化，加载规则库数据
            appUaMatcher.initialize(); //uaMap.put(DimCiRuleAppBean.getUaContains(), AppRuleCacheBean); 将bean缓存到cachebean里面去，但没有保存regex类的字段，但多了contID
            appMatcher.initialize();//AppRuleCacheBean DimCiRuleAppBean  hostMap.put(host, cacheBeanlist);
            siteMatcher.initialize();

            contActionMatcher.initialize();
            locMatcher.initialize();
            userFlagMatcher.initialize();

            blackListFilter.initialize();

            terminalMatcher.initialize();
        } catch (Exception e) {
            log.error("加载规则库失败！" + e.getMessage(), e);
        } finally {
//            DBUtil.destroyConnPool();//销毁连接池
        }
    }
}
