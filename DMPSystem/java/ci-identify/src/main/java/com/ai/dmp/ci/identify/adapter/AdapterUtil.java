package com.ai.dmp.ci.identify.adapter;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by yulei on 2015/10/21.
 */
public class AdapterUtil {

    public static AbstractAdapter getAdapter() throws Exception {
        //  初始化适配器
        String adapterClass = Config.getString(CIConst.Config.GS_PROJECT_ADAPTER);//里面有加载默认配置文件
        AbstractAdapter adapter = (AbstractAdapter) Class.forName(adapterClass).newInstance();//使用具体的adapter
        return adapter;
    }

    /**
     * 获取tableName表此次计算的所有输出路径（到分区）
     *
     * @param tableName
     * @param hasNettypePar   : 该表是否有nettype分区
     * @param otherPartitions
     * @return
     * @throws Exception
     */
    public static List<Path> getFullPathList(String tableName, boolean hasNettypePar, String... otherPartitions) throws Exception {
        List<Path> pathList = new ArrayList<Path>();
        List<String> hourIds = CIMainParam.getHourIds();
        for (String hourId : hourIds) {
            String dayId = hourId.substring(0, 8);
            String pathString = null;
            if (hasNettypePar) {
                pathString = CIMainParam.hiveHome + tableName + "/provider=" + CIMainParam.provider + "/province=" + CIMainParam.province + "/net_type=" + CIMainParam.nettype + "/day_id=" + dayId + "/hour_id=" + hourId;
            } else {
                pathString = CIMainParam.hiveHome + tableName + "/provider=" + CIMainParam.provider + "/province=" + CIMainParam.province + "/day_id=" + dayId + "/hour_id=" + hourId;
            }

            if (otherPartitions.length > 0) {
                for (String p : otherPartitions) {
                    pathString = pathString + "/" + p;
                }
            }
            pathList.add(new Path(pathString));
        }
        return pathList;
    }

    /**
     * 获取DPI样例数据的全路径
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public static List<Path> getDpiExampleDataFullPathList(String tableName) throws Exception {
        String tablePath = CIMainParam.hiveHome + tableName + "/";
        List<Path> pathList = new ArrayList<Path>();
        List<String> hourIds = CIMainParam.getHourIds();
        for (String hourId : hourIds) {
            String path = tablePath + "day_id=" + hourId.substring(0, 8) + "/hour_id=" + hourId;
            pathList.add(new Path(path));
        }
        return pathList;
    }

    /**
     * 随机指定范围内N个不重复的数
     *
     * @param min: 指定范围最小值(可能包括)
     * @param max: 指定范围最大值(可能包括)
     * @param n    : 随机数个数
     * @return
     */
    public static int[] randomArray(int min, int max, int n) {
        int len = max - min + 1;

        if (max < min || n > len) {
            return null;
        }

        //初始化给定范围的待选数组
        int[] source = new int[len];
        for (int i = min; i < min + len; i++) {
            source[i - min] = i;
        }

        int[] result = new int[n];
        Random rd = new Random();
        int index = 0;
        for (int i = 0; i < result.length; i++) {
            //待选数组0到(len-2)随机一个下标
            index = Math.abs(rd.nextInt() % len--);
            //将随机到的数放入结果集
            result[i] = source[index];
            //将待选数组中被随机到的数，用待选数组(len-1)下标对应的数替换
            source[index] = source[len];
        }
        return result;
    }

    /**
     * 将Imei标准化
     *
     * @param srcImei ： 原始15或者14为imei
     * @return： 最终的15位imei
     */
    public static String imeiStandardization(String srcImei) {
        if (srcImei == null || srcImei.length() < 14) {
            return null;
        }
        try {
            srcImei = srcImei.substring(0, 14);
            char[] imeiChar = srcImei.toCharArray();
            int checkInt = 0;
            int temp;
            int a;
            int b;
            for (int i = 0; i < imeiChar.length; i++) {
                a = Integer.parseInt(String.valueOf(imeiChar[i]));
                i++;
                temp = Integer.parseInt(String.valueOf(imeiChar[i])) * 2;
                b = temp < 10 ? temp : temp - 9;
                checkInt += a + b;
            }
            checkInt %= 10;
            checkInt = checkInt == 0 ? 0 : 10 - checkInt;
            String imei = srcImei.substring(0, 14) + checkInt;
            return imei;
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        String srcImei = "864678028230660";
        System.out.println(imeiStandardization(srcImei));
    }
}
