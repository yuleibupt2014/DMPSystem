package com.ai.dmp.ci.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Title: UrlUtil<br>
 * Description:
 * <ul>
 * <li>获取主机名：{@link #getHost(String)}；<br>
 * <li>获取一级域名：{@link #topDomain(String)}；
 * <li>判断一般URL格式正确性：{@link #isValidUrl(String)}
 * <li>获取url的后缀名：{@link #urlExtention(String)}
 * </ul>
 * <br>
 *
 * @version 1.0
 */
public class UrlUtil {
    private static Logger log = Logger.getLogger(UrlUtil.class);
    //	public static class Tester {
    //		public static void main(String[] args) throws MalformedURLException {
    //			String url = "http://www.baidu.com/1.hh/ff?ff==4%dfg7";
    //			System.out.println(UrlUtil.urlExtention(url));
    //		}
    //
    //	}

    private static Set<String> TYPE = new HashSet<String>();

    private static Set<String> COUNTRY = new HashSet<String>();

    private static Map<String, String> FILTER = new HashMap<String, String>();

    /**
     * 实际使用量为{@link #CACHE_BASE_SIZE}的9倍
     */
    private static final int CACHE_BASE_SIZE = 10000;
    @SuppressWarnings("unchecked")
    private static Map<String, String> topDomainCache = new LRUMap(CACHE_BASE_SIZE);
    @SuppressWarnings("unchecked")
    private static Map<String, Pair<String, Integer>> levelDomainCache = new LRUMap(CACHE_BASE_SIZE * 5);
    /**
     * 存域名值的缓存
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> levelCache = new LRUMap(CACHE_BASE_SIZE * 5);

    static {
        try {
            init();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化<br>
     * 从UrlUtil中获得相应的域名命名信息。
     */
    private static void init() throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
        InputStream is = FileUtil.openInputStream("classpath:/config/url-top.xml");
        if (is == null) {
            is = UrlUtil.class.getResourceAsStream("url-top.xml");
        }
        Node rootNode = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(is);
        XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList categories = (NodeList) xPath.evaluate("/domain/items[@type='top-level']/item/@name", rootNode, XPathConstants.NODESET);
        for (int i = 0; i < categories.getLength(); i++) {
            TYPE.add(categories.item(i).getNodeValue());
        }
        categories = (NodeList) xPath.evaluate("/domain/items[@type='country code']/item/@name", rootNode, XPathConstants.NODESET);
        for (int i = 0; i < categories.getLength(); i++) {
            COUNTRY.add(categories.item(i).getNodeValue());
        }

        categories = (NodeList) xPath.evaluate("/domain/items[@type='filter top-level']/item", rootNode, XPathConstants.NODESET);
        Node item = null;
        for (int i = 0; i < categories.getLength(); i++) {
            item = categories.item(i);
            FILTER.put(item.getAttributes().getNamedItem("domain").getNodeValue(), item.getAttributes().getNamedItem("topdomain").getNodeValue());
        }

    }

    /**
     * 通过host获取一级域名<br>
     * 一级域名就是包含国家域名、顶级域名、顶级和国家域名的域名，再往下一个层级。
     *
     * @param host url的host部分，如果host==null，则返回""。
     * @return 本方法不返回null
     */
    public static String topDomain(final String host) {
        // 1.查看缓存是否能够命中
        String domain = topDomainCache.get(host);
        if (domain != null) {
            return domain;
        }
        // 2.判断输入参数是否有错
        if (host == null) {
            return "";
        }
        // 3.计算一级域名
        String hostLow = host.toLowerCase();
        boolean flag = true;
        String[] items = StringUtils.split(hostLow, '.');
        for (String item : items) {
            flag &= StringUtils.isNumeric(item);
        }
        if (!flag) {
            domain = hostLow;
        }
        if (domain != null && items.length >= 3) {
            int minus1 = items.length - 1;
            int minus2 = items.length - 2;
            if (COUNTRY.contains(items[minus1])) {
                if (TYPE.contains(items[minus2])) {
                    domain = items[items.length - 3] + "." + items[minus2] + "." + items[minus1];
                } else {
                    domain = items[items.length - 2] + "." + items[minus1];
                }
            } else if (TYPE.contains(items[minus1])) {
                domain = items[minus2] + "." + items[minus1];
            }
        } else {
            domain = hostLow;
        }
        topDomainCache.put(host, domain);
        return domain;
    }

    /**
     * 得到对应级别的域名，如三级域名。<br>
     * 例外处理：
     * <ul>
     * <li>如果输入是IP，则返回IP，level是-1。
     * </ul>
     * 级别定义：
     * <ul>
     * <li>域名的级别如果既包含通用顶级域名，又包含国家顶级域名，那么在级别计算中只算作一级
     * <li>顶级域名定义为0级；
     * <li>普通的网站如baidu.com定义为1级；
     * <li>带有www的，与其他字母一样看待，即www.baidu.com算作2级
     * </ul>
     *
     * @param level 需要计算的域名最大级别，应该是一个2以上的整数
     * @return
     * @throws IllegalArgumentException
     */
    public static Pair<String, Integer> levelDomain(String host, int level) {
        // 1.查看缓存是否能够命中
        Pair<String, Integer> result;
        synchronized (levelDomainCache) {
            result = levelDomainCache.get(host + "€" + level);
        }
        if (result != null) {
            return result;
        }
        // 2.正确性校验
        if (host == null || host.length() < 3) {
            throw new IllegalArgumentException("Input host invalid: " + host);
        }
        if (level < 0) {
            throw new IllegalArgumentException("Input level invalid: " + level);
        }
        // 3.看是否是IP，如果是直接返回
        if (isIP(host)) {
            return new MutablePair<String, Integer>(host, -1);
        }

        int length = host.length();
        int lastIndex = host.lastIndexOf(".", length);
        List<String> items = new ArrayList<String>(2);
        String resultStr = null;
        for (int i = 0; (i < 2) && (lastIndex < length); i++) {
            resultStr = host.substring(lastIndex + 1, length);
            length = lastIndex;
            lastIndex = host.lastIndexOf(".", length - 1);
            items.add(resultStr);
        }

        if (items.size() == 2) {
            if (COUNTRY.contains(items.get(0)) && TYPE.contains(items.get(1))) {
                items.add(0, items.remove(1) + '.' + items.remove(0));
            }
        }
        result = new MutablePair<String, Integer>(items.get(0), 0);
        synchronized (levelDomainCache) {
            levelDomainCache.put(host + "€" + level, result);
        }
        return result;
    }

    /**
     * 判断是否是IP。
     *
     * @param host
     * @return
     */
    public static boolean isIP(String host) {
        if (host == null || host.length() < 7 || host.length() > 15) {
            return false;
        }
        int dotTimes = 0, numTimes = 0;
        for (char c : host.toCharArray()) {
            if (c == '.') {
                dotTimes++;
                if (numTimes > 3) {
                    return false;
                }
                numTimes = 0;
            } else if (c >= '0' && c <= '9') {
                numTimes++;
            } else {
                return false;
            }
        }
        if (numTimes > 3 || numTimes == 0 || dotTimes != 3) {
            return false;
        }
        return true;
    }

    /**
     * 获取主机名
     *
     * @param urlStr
     * @return
     * @throws MalformedURLException
     */
    public static String getHost(final String urlStr) throws MalformedURLException {
        URL url = null;
        url = new URL(urlStr);
        String host = url.getHost();
        return host;
    }

    /**
     * 判断一般URL格式正确性
     *
     * @param urlStr 字符串形式的URL
     * @return 若不是正确的url格式，返回null
     */
    public static URL isValidUrl(final String urlStr) {
        // 2.1 URL不能为空，长度不能为0
        if (StringUtils.isEmpty(urlStr)) {
            log.debug("Empty url: " + urlStr);
            // 2.2 URL中不能包含空格(部分url包含是可以访问的)
            // } else if (StringUtils.containsAny(urlStr, " \t")) {
            // log.error("Contains blank: " + data);
            // 2.3不能含中文
        } else if (urlStr.length() != urlStr.getBytes().length) {
            log.debug("Contains Non-ASCII char: " + urlStr);
        } else {
            URL url = null;
            try {
                // 2.4new url需要成功
                url = new URL(urlStr);
                String host = url.getHost();
                // 2.5 主机名不能为空
                if (StringUtils.isEmpty(host)) {
                    log.debug("Empty host: " + urlStr);
                    // 2.6 主机名不能以字母和数字以外的字符开头
                } else if (!CharUtils.isAsciiAlphanumeric(host.charAt(0))) {
                    log.debug("Start char is wrong: " + urlStr);
                    // 2.7 主机名中至少要包含一个”.”(有时候这个是能上的)
                } else if (!host.contains(".")) {
                    log.debug("Host less than one dot: " + urlStr);
                    // 2.8主机名中不能包含“.-”以外的特殊符号
                } else if (!StringUtils.containsOnly(host.toLowerCase(), "0123456789-_.abcdefghijklmnopqrstuvwxyz")) {
                    log.debug("host contain special char: " + urlStr);
                    // 2.9 端口号的值应该在[1-65535]
                } else if ((url.getPort() > 65535) || (url.getPort() < -1)) {
                    log.debug("Wrong url port: " + urlStr);
                } else {
                    return url;
                }
            } catch (Exception ex) {
                log.error("Wrong url of data: " + urlStr);
            }
        }
        return null;
    }

    /**
     * 判断一般URL格式正确性
     *
     * @param urlStr 字符串形式的URL
     * @return urlstr 合法的url ， null 返回 url不合法
     */
    public static String isValidUrlByString(final String urlStr) {
        // 2.1 URL不能为空，长度不能为0
        if (StringUtils.isEmpty(urlStr)) {
            log.debug("Empty url: " + urlStr);
            // 2.2 URL中不能包含空格(部分url包含是可以访问的)
            // } else if (StringUtils.containsAny(urlStr, " \t")) {
            // log.error("Contains blank: " + data);
            // 2.3不能含中文
        } else if (urlStr.length() != urlStr.getBytes().length) {
            log.debug("Contains Non-ASCII char: " + urlStr);
        } else {
            String newUrl = urlStr;
            if (!urlStr.startsWith("http://") && !urlStr.startsWith("https://")) {
                newUrl = "http://" + urlStr;
            }
            URL url = null;
            try {
                // 2.4new url需要成功
                url = new URL(newUrl);
                String host = url.getHost();
                // 2.5 主机名不能为空
                if (StringUtils.isEmpty(host)) {
                    log.debug("Empty host: " + urlStr);
                    // 2.6 主机名不能以字母和数字以外的字符开头
                } else if (!CharUtils.isAsciiAlphanumeric(host.charAt(0))) {
                    log.debug("Start char is wrong: " + urlStr);
                    // 2.7 主机名中至少要包含一个”.”(有时候这个是能上的)
                } else if (!host.contains(".")) {
                    log.debug("Host less than one dot: " + urlStr);
                    // 2.8主机名中不能包含“.-”以外的特殊符号
                } else if (!StringUtils.containsOnly(host.toLowerCase(), "0123456789-_.abcdefghijklmnopqrstuvwxyz")) {
                    log.debug("host contain special char: " + urlStr);
                    // 2.9 端口号的值应该在[1-65535]
                } else if ((url.getPort() > 65535) || (url.getPort() < -1)) {
                    log.debug("Wrong url port: " + urlStr);
                } else {
                    return newUrl;
                }
            } catch (Exception ex) {
                log.debug("Wrong url of data: " + urlStr);
            }
        }
        return null;
    }

    /**
     * 通过url的path部分计算url的后缀名<br>
     *
     * @param urlPath
     * @return 如果计算不出后缀名，则返回“”，本方法不返回null
     * @throws MalformedURLException
     */
    public static String urlExtention(final String urlPath) throws MalformedURLException {
        URL url = new URL(urlPath);
        String extention = FilenameUtils.getExtension(url.getPath());
        if (extention == null) {
            extention = "";
        }
        return extention;
    }

    /**
     * 直接得到对应级别的域名值。<br>
     * 例外处理：
     * <ul>
     * <li>如果输入是IP，则返回IP，level是-1。
     * </ul>
     * 例如：
     * <ul>
     * <li>video.sina.com.cn,2 输出结果是video
     * <li>finance.video.sina.com.cn,3 输出的结果是finance
     * </ul>
     *
     * @param level = 0 ,取顶级域名
     * @return
     * @throws IllegalArgumentException
     */
    public static String levelDomainValue(String host, int level) {
        // 1.查看缓存是否能够命中,如果规则库中的完整域名是不重复的，就没有必要加cache
        String result;
        synchronized (levelCache) {
            result = levelCache.get(host + "€" + level);
        }
        if (result != null) {
            return result;
        }
        // 2.正确性校验
        if (host == null || host.length() < 3) {
            throw new IllegalArgumentException("Input host invalid: " + host);
        }
        if (level < 0) {
            throw new IllegalArgumentException("Input level invalid: " + level);
        }
        // 3.看是否是IP，如果是直接返回
        if (isIP(host)) {
            return null;
        }
        // 4.计算
        StringBuilder sb = new StringBuilder(host.length());
        List<String> items = new ArrayList<String>(level + 1);
        char[] cs = host.toCharArray();
        boolean top = false;
        for (int i = cs.length - 1; i >= 0; i--) {
            if (cs[i] == '.') {
                items.add(sb.reverse().toString());
                sb.setLength(0);
                // 等于2的时候需要判断是否能将域名合并成一个顶级域名
                if (items.size() == 2 && !top) {
                    if (COUNTRY.contains(items.get(0)) && TYPE.contains(items.get(1))) {
                        items.add(0, items.remove(1) + '.' + items.remove(0));
                        top = true;
                    }
                }
                if ((items.size() > 1) && (items.size() - 1 == level)) {
                    break;
                }
            } else {
                sb.append(cs[i]);
            }
        }

        String last = sb.reverse().toString();
        // www开头但并非域名，所以排除掉
        if (!"www".equals(last))
            items.add(last);

        if (items.size() - 1 >= level) {
            result = items.get(level);
        } else {
            result = null;
        }
        synchronized (levelCache) {
            levelCache.put(host + "€" + level, result);
        }
        return result;
    }

    /**
     * 过滤顶级域名
     *
     * @param completeDomain
     * @return
     */
    public static String filterTopDomin(String completeDomain) {
        return FILTER.get(completeDomain);
    }

    public static String[] getTypeArr() {
        Iterator<String> it = UrlUtil.TYPE.iterator();
        String[] result = new String[UrlUtil.TYPE.size()];
        int i = 0;
        while (it.hasNext()) {
            result[i] = it.next();
            i++;
        }
        return result;
    }

    public static String[] getCountryArr() {
        Iterator<String> it = UrlUtil.COUNTRY.iterator();
        String[] result = new String[UrlUtil.COUNTRY.size()];
        int i = 0;
        while (it.hasNext()) {
            result[i] = it.next();
            i++;
        }
        return result;
    }

    /**
     * 解析URL的参数
     *
     * @param url
     * @return
     * @throws Exception
     */
    public static Map<String, String> parseUrlParam(String url) throws Exception {
        Map<String, String> paramMap = new HashMap<String, String>();
        if (StringUtil.isEmpty(url) || !url.startsWith("http://")) {
            return paramMap;
        }
        String path = new URL(url).getQuery();
        if (StringUtil.isEmpty(path)) {
            return paramMap;
        }
        String[] params = path.split("&");
        int idx = 0;
        for (String param : params) {
            idx = param.indexOf("=");
            if (idx > 0 && idx < param.length() - 1) {
                paramMap.put(param.substring(0, idx), param.substring(idx + 1));
            }
        }
        return paramMap;
    }

    /**
     * 解析cookie参数
     *
     * @param cookie
     * @return
     * @throws Exception
     */
    public static Map<String, String> parseCookieParam(String cookie) throws Exception {
        Map<String, String> paramMap = new HashMap<String, String>();
        if (StringUtil.isEmpty(cookie)) {
            return paramMap;
        }

        String[] params = cookie.split(CIConst.Separator.SEMICOLON);//根据分号分割
        String[] keyValue = null;
        for (String param : params) {
            keyValue = param.split(CIConst.Separator.EQUAL);
            if (keyValue.length == 2) {
                paramMap.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }

        return paramMap;
    }

    /**
     * 过滤,格式化,字符串
     *
     * @param sourceStr
     * @return
     */
    public static String jsonCharFilter(String sourceStr, String spaceString) {
        sourceStr = sourceStr.replaceAll("\\|", spaceString);
        sourceStr = sourceStr.replaceAll("\\$", spaceString);
        sourceStr = sourceStr.replaceAll("\b", spaceString);
        sourceStr = sourceStr.replaceAll("\t", spaceString);
        sourceStr = sourceStr.replaceAll("\n", spaceString);
        sourceStr = sourceStr.replaceAll("\f", spaceString);
        sourceStr = sourceStr.replaceAll("\r", spaceString);
        sourceStr = sourceStr.replaceAll("\\\\", spaceString);
        sourceStr = sourceStr.replaceAll("\"", spaceString);
        return sourceStr;
    }
}
