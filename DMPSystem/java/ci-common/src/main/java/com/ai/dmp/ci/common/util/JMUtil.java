package com.ai.dmp.ci.common.util;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.decoder.BASE64Decoder;
import com.ai.dmp.ci.common.decoder.BASE64Encoder;

public class JMUtil
{
    private static Logger log = Logger.getLogger(JMUtil.class);

    private static String charset = "utf-8";

    public static void main(String[] args) throws Exception
    {
        String str = "hadoop\u0002程祥峰\u0001";
        System.out.println(str);
        String jmStr = encrypt(str);
        System.out.println(jmStr);
        //		System.out.println(reCryptograph(jmStr));
        System.out.println(decrypt(jmStr));
        System.out.println(decrypt("b21hLGVva2ZgLHFxY3JyY3UC"));
    } 

    // 加密方法
    public static String encrypt(String strOriginal)
    {
        BASE64Encoder encoder = new BASE64Encoder();

        char charTemp;
        int intCrpytion;//
        int intRnd; // 加密随机数
        intRnd = (int) (100 * Math.random() + 1);
        StringBuffer strOriToB = new StringBuffer(strOriginal);// 将String参数转换为StringBuffer

        /*
         * 加密算法：将字符串先进行反转，对每个字符强制取整后和产生的随机数进行按位异或，再把随机数对应的字符连接到字符串的尾部。
         */
        StringBuilder resultBuilder = new StringBuilder("");
        strOriToB = strOriToB.reverse();// 反转字符串
        for (int i = 0; i < strOriToB.length(); i++)
        {
            charTemp = strOriToB.charAt(i);
            intCrpytion = (int) charTemp;
            intCrpytion = intCrpytion ^ (intRnd % 32);
            resultBuilder.append((char) intCrpytion);
        }
        resultBuilder.append((char) intRnd);

        byte[] byteArr = null;
        try
        {
            byteArr = resultBuilder.toString().getBytes(charset);
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }

        return encoder.encode(byteArr);
    }

    // 解密方法
    public static String decrypt(String strCryptograph) throws Exception
    {
        try
        {
            if (StringUtil.isEmpty(strCryptograph))
            {
                return strCryptograph;
            }
            BASE64Decoder decoder = new BASE64Decoder();
            strCryptograph = new String(decoder.decodeBuffer(strCryptograph, charset), charset);
            String strReturn = new String();
            int intTemp;
            int intCrypt;
            String strTemp = new String();
            intTemp = (int) strCryptograph.charAt(strCryptograph.length() - 1);
            strTemp = strCryptograph.substring(0, strCryptograph.length() - 1);
            for (int i = 0; i < strTemp.length(); i++)
            {
                intCrypt = (int) strTemp.charAt(i);
                intCrypt = intCrypt ^ (intTemp % 32);
                strReturn += (char) intCrypt;
            }
            StringBuffer strRe = new StringBuffer(strReturn);
            strRe = strRe.reverse();// 反转字符串
            return strRe.toString();
        }
        catch (Exception e)
        {
            log.error("解密字符串失败！" + strCryptograph + "\n" + e.getMessage(), e);
            throw e;
        }
    }
}