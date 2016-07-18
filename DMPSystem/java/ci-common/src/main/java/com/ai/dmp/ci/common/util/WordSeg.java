package com.ai.dmp.ci.common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单词分词
 */
public class WordSeg {

    private static Map<Character, Character> segMap = new HashMap<Character, Character>();//分隔符

    static {
        segMap.put(' ', null);
        segMap.put('/', null);
        segMap.put(';', null);
        segMap.put('(', null);
        segMap.put(')', null);
        segMap.put(',', null);
        segMap.put('-', null);
    }

    public static void main(String[] args) {
        String ua = "Mozilla/5.0 (Linux; U; Android 4.0.3; zh_CN; Coolpad 8079; Build/IML74K) cn.ninegame.gamemanager/18; NineGameClient/android ve/2 dxxx  xx";
        List<String> list = wordSeg(ua);
        for (String str : list) {
            System.out.println(str+"  "+str.length());
        }
    }

    /**
     * 分词,分隔符：" ","/",";","(",")"
     *
     * @param src
     * @return
     */
    public static List<String> wordSeg(String src) {
        List<String> list = new ArrayList<String>();
        if (StringUtil.isEmpty(src)) {
            return list;
        }

        char[] srcCharArr = src.toCharArray();
        int start = 0;
        int length = srcCharArr.length;
        for (int i = 0; i < length; i++) {
            if (segMap.containsKey(srcCharArr[i])) {
                if (i - start > 1) { //分割的单词至少为两个字符
                    list.add(src.substring(start, i));
                }
                start = i + 1;
            }
            if(i == length - 1 && i - start >= 1){
                list.add(src.substring(start, i+1));
            }
        }
        return list;
    }

}
