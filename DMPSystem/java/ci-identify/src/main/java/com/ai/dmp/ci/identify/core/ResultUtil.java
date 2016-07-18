package com.ai.dmp.ci.identify.core;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yulei on 2015/10/30.
 */
public class ResultUtil {
    // 输出字段分隔符：  “|”
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);

    /**
     * 获取原始数据值
     *
     * @param result
     * @return
     */
    public static String getValue_S(Result result) {
        StringBuilder resultBuilder = new StringBuilder("");
        for (int i = 0; i < Config.inputColList.size(); i++) {
            resultBuilder.append(result.get(Config.inputColList.get(i))).append(SEPARATOR);
        }
        String outStr = resultBuilder.toString();
        outStr = outStr.substring(0, outStr.length() - SEPARATOR.length());
        return outStr;
    }

    public static List<String> getTrack(Result result) {
        String appId = result.get(CIConst.ResultColName.APP_ID);
        String siteId = result.get(CIConst.ResultColName.SITE_ID);

        List<String> resultlist = new ArrayList<String>();
        if (StringUtil.isEmpty(appId) || StringUtil.isEmpty(siteId) || result.getContActionList().size() == 0) {
            return resultlist;
        }

        return resultlist;
    }

    /**
     * 明细表数据
     *
     * @param result
     * @return
     */
    public static List<String> getDmpCiBhValue(Result result) {
        List<String> resultlist = new ArrayList<String>();
        StringBuilder resultBuilder = null;
        List<ContActionEntry> list = result.getContActionList();

        String src = getValue_S(result);
        String enhance = _getEnhanceValue(result); //只是获取cont_id之前的字段
        if (list.size() == 0) {
            resultBuilder = new StringBuilder(src).append(SEPARATOR);
            resultBuilder.append(enhance).append(SEPARATOR);
            resultBuilder.append("").append(SEPARATOR);
            resultBuilder.append("").append(SEPARATOR);
            resultBuilder.append("").append(SEPARATOR);
            resultBuilder.append("").append(SEPARATOR);
            resultBuilder.append("");
            resultlist.add(resultBuilder.toString());
        } else {
            for (int i = 0; i < list.size(); i++) {
                ContActionEntry entry = list.get(i);
                resultBuilder = new StringBuilder(src).append(SEPARATOR);
                resultBuilder.append(enhance).append(SEPARATOR);

                resultBuilder.append(entry.getContId()).append(SEPARATOR);
                resultBuilder.append(entry.getActionId()).append(SEPARATOR);
                resultBuilder.append(entry.getValue()).append(SEPARATOR);
                resultBuilder.append(entry.getValueTypeId()).append(SEPARATOR);

                resultBuilder.append(i == 0 ? "" : "1");

                resultlist.add(resultBuilder.toString());
            }
        }
        return resultlist;
    }


    //获取增强字段（cont_id字段之前的值）
    private static String _getEnhanceValue(Result result) {
        StringBuilder resultBuilder = new StringBuilder("");
        for (int i = 0; i < Config.enhanceColList.size(); i++) {
            String col = Config.enhanceColList.get(i);
            if (col.equals(CIConst.ResultColName.CONT_ID)
                    || col.equals(CIConst.ResultColName.ACTION_ID)
                    || col.equals(CIConst.ResultColName.VALUE)
                    || col.equals(CIConst.ResultColName.VALUE_TYPE_ID)
                    || col.equals(CIConst.ResultColName.RECORD_TYPE)) {
                break;
            }
            resultBuilder.append(result.get(Config.enhanceColList.get(i))).append(SEPARATOR);
        }

        String outStr = resultBuilder.toString();
        outStr = outStr.substring(0, outStr.length() - SEPARATOR.length());
        return outStr;
    }

}
