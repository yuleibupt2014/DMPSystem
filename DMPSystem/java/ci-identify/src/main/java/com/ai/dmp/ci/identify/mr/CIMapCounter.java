package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.List;

/**
 * Created by yulei on 2015/10/22.
 */
public class CIMapCounter {
    private static String COUNTER_TYPE = "DMP-CI-Records";// 计数器大类

    public static void printExceptionCounter(Mapper.Context context) {
        context.getCounter(COUNTER_TYPE, "Exception records").increment(1);
    }

    /**
     * 计数器，方便调试
     *
     * @param context
     * @param result
     */
    public static void printDetailCounter(Mapper.Context context, Result result) {
        // map输出条数计数器
        context.getCounter(COUNTER_TYPE, "Success").increment(1);

        //过滤的记录数计数器
        if (result.ifCleaned()) {
            context.getCounter(COUNTER_TYPE, "data clean").increment(1);
        }

        // APP识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.APP_ID))) {
            context.getCounter(COUNTER_TYPE, "AppId").increment(1);
        }

        // 站点识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.SITE_ID))) {
            context.getCounter(COUNTER_TYPE, "Site_id").increment(1);
        }

        //内容行为标签计数器
        List<ContActionEntry> contActionList = result.getContActionList();
        for (ContActionEntry entry : contActionList) {
            if (!StringUtils.isEmpty(entry.getContId())) {
                context.getCounter(COUNTER_TYPE, "Cont_id").increment(1);
            }

            if (!StringUtils.isEmpty(entry.getActionId())) {
                context.getCounter(COUNTER_TYPE, "Action_id").increment(1);
            }

            if (!StringUtils.isEmpty(entry.getValue())) {
                context.getCounter(COUNTER_TYPE, "Value").increment(1);
            }
        }

        // Device_model识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.DEVICE_MODEL))) {
            context.getCounter(COUNTER_TYPE, "Device_model").increment(1);
        }

        // Device_type识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.DEVICE_TYPE))) {
            context.getCounter(COUNTER_TYPE, "Device_type").increment(1);
        }

        // Device_os识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.DEVICE_OS))) {
            context.getCounter(COUNTER_TYPE, "Device_os").increment(1);
        }

        // Device_browser识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.DEVICE_BROWSER))) {
            context.getCounter(COUNTER_TYPE, "Device_browser").increment(1);
        }

        // 电话识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.PHONE_NO))) {
            context.getCounter(COUNTER_TYPE, "PhoneNo").increment(1);
        }

        // Imei识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.IMEI))) {
            context.getCounter(COUNTER_TYPE, "Imei").increment(1);
        }

        // Imsi识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.IMSI))) {
            context.getCounter(COUNTER_TYPE, "Imsi").increment(1);
        }

        // Mac识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.MAC))) {
            context.getCounter(COUNTER_TYPE, "Mac").increment(1);
        }

        // Idfa识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.IDFA))) {
            context.getCounter(COUNTER_TYPE, "Idfa").increment(1);
        }

        // Android_id识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.ANDROID_ID))) {
            context.getCounter(COUNTER_TYPE, "Android_id").increment(1);
        }

        // cookie_id识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.COOKIE_ID))) {
            context.getCounter(COUNTER_TYPE, "CookieId").increment(1);
        }
        // user_name识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.USER_NAME))) {
            context.getCounter(COUNTER_TYPE, "UserName").increment(1);
        }
        // email识别计数器
        if (!StringUtils.isEmpty(result.get(CIConst.ResultColName.EMAIL))) {
            context.getCounter(COUNTER_TYPE, "Email").increment(1);
        }
    }
}
