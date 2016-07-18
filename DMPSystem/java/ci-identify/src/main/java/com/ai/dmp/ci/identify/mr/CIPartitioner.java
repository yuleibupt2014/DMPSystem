package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yulei on 2015/11/12.
 */
public class CIPartitioner extends HashPartitioner {

    //将固定的用户标志数据使用固定的reduce进行处理
    private static Map<String, Integer> userfagMap = new HashMap<String, Integer>();
    private static String SEPARATOR = "\t";

    static {
        userfagMap.put(CIConst.OutputPri.S_IMEI, 0);
        userfagMap.put(CIConst.OutputPri.S_IMSI, 1);
        userfagMap.put(CIConst.OutputPri.S_IDFA, 2);
        userfagMap.put(CIConst.OutputPri.PHONE_NO, 3);
        userfagMap.put(CIConst.OutputPri.IMEI, 4);
        userfagMap.put(CIConst.OutputPri.IMSI, 5);
        userfagMap.put(CIConst.OutputPri.MAC, 6);
        userfagMap.put(CIConst.OutputPri.IDFA, 7);
        userfagMap.put(CIConst.OutputPri.ANDROID_ID, 8);
        userfagMap.put(CIConst.OutputPri.USER_NAME, 9);
        userfagMap.put(CIConst.OutputPri.EMAIL, 10);
    }

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
        String keyStr = key.toString();
        String pri = keyStr.substring(0, keyStr.indexOf(SEPARATOR));
        if (numPartitions > userfagMap.size()) {
            if (pri.equals(CIConst.OutputPri.S_IMEI) || pri.equals(CIConst.OutputPri.S_IMSI)
                    || pri.equals(CIConst.OutputPri.S_IDFA) || pri.equals(CIConst.OutputPri.PHONE_NO)
                    || pri.equals(CIConst.OutputPri.IMEI) || pri.equals(CIConst.OutputPri.IMSI)
                    || pri.equals(CIConst.OutputPri.MAC) || pri.equals(CIConst.OutputPri.IDFA)
                    || pri.equals(CIConst.OutputPri.ANDROID_ID) || pri.equals(CIConst.OutputPri.USER_NAME)
                    || pri.equals(CIConst.OutputPri.EMAIL)) {
                return userfagMap.get(pri);
            }
        }
        return super.getPartition(key, value, numPartitions);
    }

}
