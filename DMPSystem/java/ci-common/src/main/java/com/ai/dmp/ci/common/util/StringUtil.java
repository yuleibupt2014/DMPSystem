package com.ai.dmp.ci.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    //需要过滤的特殊字符串
    private static final Map<Character, String> filterStringMap = new HashMap<Character, String>();

    static {
        filterStringMap.put('\t', null);
        filterStringMap.put('\b', null);
        filterStringMap.put('\f', null);
        filterStringMap.put('\r', null);
        filterStringMap.put('\n', null);
        filterStringMap.put('\u0001', null);
        filterStringMap.put('\u0002', null);
        filterStringMap.put('\u0003', null);
        filterStringMap.put('\u0004', null);
    }

    /**
     * 判断字符串是否空串
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        if (str == null || "".equals(str)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 传入null，则返回""；如果传入非null字符串，则去掉首尾的空格
     *
     * @param str
     * @return
     */
    public static String exceptNull(String str) {
        if (str == null || "".equals(str.trim())) {
            str = "";
        }
        return str.trim();
    }

    /**
     * 判断str是否是纯数字串
     *
     * @param str
     * @return
     */
    public static boolean isNumber(String str) {
        Pattern p = Pattern.compile("^[0-9]+$");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 过滤非法字符
     * @param sourceStr
     * @param spaceString
     * @return
     */
    @Deprecated
    public static String filterChar1(String sourceStr, String spaceString) {
        sourceStr = sourceStr.replaceAll("\b", spaceString);
        sourceStr = sourceStr.replaceAll("\t", spaceString);
        sourceStr = sourceStr.replaceAll("\n", spaceString);
        sourceStr = sourceStr.replaceAll("\f", spaceString);
        sourceStr = sourceStr.replaceAll("\r", spaceString);
        sourceStr = sourceStr.replaceAll("\u0001", spaceString);
        sourceStr = sourceStr.replaceAll("\u0002", spaceString);
        sourceStr = sourceStr.replaceAll("\u0003", spaceString);
        sourceStr = sourceStr.replaceAll("\u0004", spaceString);
//        sourceStr = sourceStr.replaceAll("\u0005", spaceString);
        return sourceStr;
    }

    public static String filterChar(String sourceStr, char sep) {
        char[] arr = sourceStr.toCharArray();
        for (int i = 0; i < arr.length; i++) {
            if (filterStringMap.containsKey(arr[i])) {
                arr[i] = sep;
            }
        }
        return new String(arr);
    }
}
