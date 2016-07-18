package com.ai.dmp.ci.identify.core.matcher;

import com.ai.dmp.ci.common.util.CharsetUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.DaoUtil;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by yulei on 2015/10/22.
 */
public class MatcherUtil {
    // 判断URL是否有GBK等字样
    private static Pattern charsetPattern = Pattern.compile("=(gbk|GBK)(\\&|$)");

    public static List<DimAppBean> dimAppList = null;
    public static List<DimCiRuleAppBean> appRuleList = null;
    public static List<DimCiRuleContActionBean> contActionRuleList = null;
    public static List<DimCiRuleLocBean> locRuleList = null;
    public static List<DimCiRuleSiteBean> siteRuleList = null;
    public static List<DimCiRuleTerminalBean> terminalList = null;
    public static List<DimCiRuleUserFlagBean> userFlagList = null;
    public static List<DimUaKwBean> uaKwList = null;

    public static void main(String[] args) throws Exception {
//        String name = parseKw("  ","aaa%3Aaaa%3A");
//        System.out.println("name = " + name);
        String kw = "%CE%C0%C9%FA%BD%ED%20%CB%D5%B7%C6";
//        kw = "yue%E2%80%86guo%E2%80%86q";
////        kw = "ab%20c";
//        System.out.println(URLDecoder.decode(kw, "utf8"));
//        System.out.println(URLDecoder.decode(kw, "gbk"));
//        System.out.println(URLDecoder.decode(kw, "gbk").length());
//        System.out.println("isUtf8:" + CharsetUtil.isUtf8(kw));
//        System.out.println("isgbk:" + CharsetUtil.isGBK(kw));
//        System.out.println(URLEncoder.encode("a bc", "gbk"));
        System.out.println(parseKw("http://suggest.taobao.com/sug?code=utf-8&extras=1&q=%E4%BA%BA%E7%94%9F&callback=jsonp1", "word%e4%b8%ad%e7%bc%96%e5%8f%b7%e7%9a%84%e7%ac%ac%e4%ba%8c%e8%a1%8c%e6%80%8e%e4%b9%88%e7%bc%a9"));
    }

    /**
     * 解析关键字
     *
     * @param url
     * @param srcKw ：原始关键字
     * @return ：解析后的关键字
     * @throws Exception
     */
    public static String parseKw(String url, String srcKw) throws Exception {
        String keyWordResult = "";
        String charset = "GBK";
        // 如果在URL中已经指明了编码格式，那么先判断是不是指定的编码格式，若不是则再判断是不是UTF-8
        if (!StringUtil.isEmpty(url) && charsetPattern.matcher(url).find()) {
            if (CharsetUtil.isGBK(srcKw)) {
                keyWordResult = URLDecoder.decode(srcKw, charset);
            } else if (CharsetUtil.isUtf8(srcKw)) {
                charset = "UTF8";
                keyWordResult = URLDecoder.decode(srcKw, charset);
            } else {
                keyWordResult = URLDecoder.decode(srcKw, charset);
            }
        } else {
            if (CharsetUtil.isUtf8(srcKw)) {
                charset = "UTF8";
                keyWordResult = URLDecoder.decode(srcKw, charset);
            } else if (CharsetUtil.isGBK(srcKw)) {
                keyWordResult = URLDecoder.decode(srcKw, charset);
            } else {
                keyWordResult = URLDecoder.decode(srcKw, charset);
            }
        }
        return keyWordResult;
    }

    /**
     * 加载规则库
     * @throws Exception
     */
    public static void loadRule()throws Exception{
        dimAppList = DaoUtil.ciDao.queryAllDimAppBean();
        appRuleList = DaoUtil.ciDao.queryAllDimCiRuleAppBean();
        contActionRuleList = DaoUtil.ciDao.queryAllDimCiRuleContActionBean();
        locRuleList = DaoUtil.ciDao.queryAllDimCiRuleLocBean();
        siteRuleList = DaoUtil.ciDao.queryAllDimCiRuleSiteBean();
        terminalList = DaoUtil.ciDao.queryAllDimCiRuleTerminalBean();
        userFlagList = DaoUtil.ciDao.queryAllDimCiRuleUserFlagBean();
        uaKwList = DaoUtil.ciDao.queryAllDimUaKwBeanBean();
    }

    public static void clearRule(){
        dimAppList = null;
        appRuleList = null;
        contActionRuleList = null;
        locRuleList = null;
        siteRuleList = null;
        terminalList = null;
        userFlagList = null;
        uaKwList = null;
    }
}
