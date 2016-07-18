package com.ai.dmp.ci.common.util;

import org.apache.log4j.Logger;

/**
 * 判断字符集(判断字符串是UTF8编码还是JBK编码)
 */
public class CharsetUtil
{
    private static Logger log = Logger.getLogger(CharsetUtil.class);

    private final static int GBK_MIN = 0x8140;// GBK编码最小值
    private final static int GBK_MAX = 0xFEFE;// GBK编码最大值
    private final static int GBK_EXCEPT_LOW = 0x7F;//GBK除去7F一条线

    private final static int UTF8_00 = 0x00;
    private final static int UTF8_7F = 0x7F;
    private final static int UTF8_80 = 0x80;
    private final static int UTF8_BF = 0xBF;
    private final static int UTF8_C0 = 0xC0;
    private final static int UTF8_DF = 0xDF;
    private final static int UTF8_E0 = 0xE0;
    private final static int UTF8_EF = 0xEF;
    private final static int UTF8_F0 = 0xF0;
    private final static int UTF8_F7 = 0xF7;

    /**
     * 判断字符串是否UTF8编码
     *
     * 16进制范围
    	0xxxxxxx								第一字段=>	00-7F
    	110xxxxx 10xxxxxx(80-BF)    			第一字段=> C0-DF
    	1110xxxx 10xxxxxx 10xxxxxx 		 		第一字段=> E0-EF
    	11110xxx 10xxxxxx 10xxxxxx 10xxxxxx 	第一字段=> F0-F7
     * @return
     */
    public static boolean isUtf8(String srcString)
    {
        String tmpString = srcString;
        try
        {
            int startint = srcString.indexOf("%");
            if (startint == -1)
                return true;// 没有”%“

            srcString = srcString.substring(startint + 1);
            if (srcString == null || srcString.length() < 1)
                return false;// 没有参数

            String[] split = srcString.split("%");
            for (int i = 0; i < split.length; i++)
            {
                if (split[i].length() < 2)
                    return false;
                split[i] = split[i].substring(0, 2);
            }
            int one;
            for (int i = 0; i < split.length;)
            {
                one = Integer.parseInt(split[i], 16);
                if (one >= UTF8_00 && one <= UTF8_7F)
                {//一个字节情况
                    i++;
                }
                else if (one >= UTF8_C0 && one <= UTF8_DF)
                {//二个字节情况
                    if (!betwen_80_BF(split[i + 1]))
                    {
                        return false;
                    }
                    i += 2;
                }
                else if (one >= UTF8_E0 && one <= UTF8_EF)
                {//三个字节情况
                    for (int j = 1; j <= 2; j++)
                    {
                        if (!betwen_80_BF(split[i + j]))
                        {
                            return false;
                        }
                    }
                    i += 3;
                }
                else if (one >= UTF8_F0 && one <= UTF8_F7)
                {//四个字节情况
                    for (int j = 1; j <= 3; j++)
                    {
                        if (!betwen_80_BF(split[i + j]))
                        {
                            return false;
                        }
                    }
                    i += 4;
                }
                else
                {
                    return false;
                }
            }
            return true;
        }
        catch (Exception e)
        {
            log.warn("判断是否UTF8编码失败，param=" + tmpString);
            return false;
        }
    }

    private static boolean betwen_80_BF(String oxString)
    {
        int ox = Integer.parseInt(oxString, 16);
        if (ox >= UTF8_80 && ox <= UTF8_BF)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * 判断字符串是否GBK编码
     *
     * GBK 采用双字节表示<br>
     * 首字节在 81-FE 之间，尾字节在 40-FE 之间<br>
     * 总体编码范围为 8140-FEFE<br>
     * a.二进制范围：10000001 01000000~11111110 11111110 b.剔除 xx7F部分<br>
     * @return
     */
    public static boolean isGBK(String srcString)
    {
        String tmpString = srcString;
        try
        {
            int startint = srcString.indexOf("%");
            if (startint == -1)
                return true;// 没有”%“

            srcString = srcString.substring(startint + 1);
            if (srcString == null || srcString.length() < 1)
                return false;// 没有参数

            String[] split = srcString.split("%");
            if (split.length % 2 != 0)
            {// 防止有基数个%的错误编码
                return false;
            }

            int ox;// 16进制
            int low;// 低8为的16进制
            for (int i = 0; i < split.length; i += 2)
            {
                if (split[i].length() < 2)
                    return false;
                if (split[i + 1].length() < 2)
                    return false;
                split[i] = split[i].substring(0, 2);
                split[i + 1] = split[i + 1].substring(0, 2);

                ox = Integer.parseInt(split[i] + split[i + 1], 16);
                low = Integer.parseInt(split[i + 1], 16);

                // 剔除 xx7F部分
                if (low == GBK_EXCEPT_LOW)
                    return false;

                if (ox < GBK_MIN || ox > GBK_MAX)
                {
                    return false;
                }
            }
            return true;
        }
        catch (Exception e)
        {
            log.warn("判断是否GBK编码失败，srcString=" + tmpString);
            return false;
        }
    }
}