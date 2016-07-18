package com.ai.dmp.ci.identify.adapter.output.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.output.AbstractReduceOutput;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 * Created by yulei on 2015/10/30.
 */
public class MobileReduceOutput extends AbstractReduceOutput {
    private static Logger log = Logger.getLogger(MobileReduceOutput.class);

    // 输出字段分隔符
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);

    Text keyText = new Text();
    Text valueText = new Text();

    /**
     * reduce阶段输出
     *
     * @param key
     * @param values
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void output(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception {
        String keyStr = key.toString();
        if (StringUtil.isEmpty(getSPhoneNo(keyStr))) {
            return;
        }
        if (keyStr.startsWith(CIConst.OutputPri.CONT_ACTION)) {
            writeTags(CIConst.OutputPri.CONT_ACTION, CIConst.TableName.DMP_UC_TAGS_M_BH, key, values, null, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.LOC_ID)) {
            writeTags(CIConst.OutputPri.LOC_ID, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.LOC_ID, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.FLOW)) {
            writeTags(CIConst.OutputPri.FLOW, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.FLOW, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.LAC_CI)) {
            writeTags(CIConst.OutputPri.LAC_CI, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.LAC_CI, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.S_IMEI) || keyStr.startsWith(CIConst.OutputPri.S_IMSI)
                || keyStr.startsWith(CIConst.OutputPri.S_IDFA) || keyStr.startsWith(CIConst.OutputPri.PHONE_NO)
                || keyStr.startsWith(CIConst.OutputPri.IMEI) || keyStr.startsWith(CIConst.OutputPri.IMSI)
                || keyStr.startsWith(CIConst.OutputPri.MAC) || keyStr.startsWith(CIConst.OutputPri.IDFA)
                || keyStr.startsWith(CIConst.OutputPri.ANDROID_ID) || keyStr.startsWith(CIConst.OutputPri.USER_NAME)
                || keyStr.startsWith(CIConst.OutputPri.EMAIL)) {//用户标识， 包括:s_imei,s_imsi等
            writeUserFlag(key, values, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.DEVICE_MODEL)) {
            writeTags(CIConst.OutputPri.DEVICE_MODEL, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.DEVICE_MODEL, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.DEVICE_TYPE)) {
            writeTags(CIConst.OutputPri.DEVICE_TYPE, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.DEVICE_TYPE, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.DEVICE_OS)) {
            writeTags(CIConst.OutputPri.DEVICE_OS, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.DEVICE_OS, multipleOutputs, context);
        } else if (keyStr.startsWith(CIConst.OutputPri.DEVICE_BROWSER)) {
            writeTags(CIConst.OutputPri.DEVICE_BROWSER, CIConst.TableName.DMP_UC_OTAGS_M_BH, key, values,
                    "sa_id=" + CIConst.OutputPri.DEVICE_BROWSER, multipleOutputs, context);
        }
    }

    /**
     * @param key
     * @param values
     * @param multipleOutputs :如果为null，则表明是combiner
     * @param context
     * @throws Exception
     */
    private void writeUserFlag(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception {
        String keyStr = key.toString();
        valueText.set("");
        if (multipleOutputs == null) { //commbiner
            context.write(key, valueText);
        } else { //reduce
            String hourId = getHourId(keyStr);
            String dayId = hourId.substring(0, 8);
            String outputPri = keyStr.substring(0, keyStr.indexOf(SEPARATOR));
            keyText.set(getKeyValue(keyStr, outputPri));
            String path = getHdfsPath(dayId, hourId, context, CIConst.TableName.DMP_CI_USER_M_BH, "mflag=" + outputPri);
            multipleOutputs.write(keyText, valueText, path);
        }
    }

    /**
     * @param outputPri
     * @param tableName
     * @param key
     * @param values
     * @param saId
     * @param multipleOutputs : 如果为null，则表明是combiner
     * @param context
     * @throws Exception
     */
    private void writeTags(String outputPri, String tableName, Text key, Iterable<Text> values, String saId,
                           MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception {
        String keyStr = key.toString();

        if (multipleOutputs == null) {  //commbiner
            Value value = computeCombinerValue(values);
            StringBuilder sb = new StringBuilder();
            sb.append(value.minTimestamp).append(SEPARATOR);
            sb.append(value.lastTimestamp).append(SEPARATOR);
            sb.append(value.count);
            valueText.set(sb.toString());

            context.write(key, valueText);
        } else { //reduce
            String hourId = getHourId(keyStr);
            String dayId = hourId.substring(0, 8);
            String keyValue = getKeyValue(keyStr, outputPri);
            keyText.set(keyValue);

            Value value = computeReduceValue(values);
            StringBuilder sb = new StringBuilder();
            sb.append(value.lastTimestamp - value.minTimestamp).append(SEPARATOR);
            sb.append(value.lastTimestamp).append(SEPARATOR);
            sb.append(value.count);
            valueText.set(sb.toString());
            String path = getHdfsPath(dayId, hourId, context, tableName, saId);
            multipleOutputs.write(keyText, valueText, path);
        }
    }

    /**
     * 计算最后访问时间、时间差、次数等
     *
     * @param values values格式：start_timestamp count
     * @return
     */
    private Value computeCombinerValue(Iterable<Text> values) {
        Value value = new Value();
        for (Text v : values) {
            String[] valueColArr = v.toString().split(CIConst.Separator.Tab, -1);
            long timestamp = Long.valueOf(valueColArr[0]);
            value.minTimestamp = value.minTimestamp < timestamp && value.minTimestamp > 0 ? value.minTimestamp : timestamp;
            value.lastTimestamp = value.lastTimestamp < timestamp ? timestamp : value.lastTimestamp;
            value.count = value.count + Integer.valueOf(valueColArr[1]);
        }
        return value;
    }

    /**
     * 计算最后访问时间、时间差、次数等
     *
     * @param values values格式：min_timestamp  max_timestamp  count
     * @return
     */
    private Value computeReduceValue(Iterable<Text> values) {
        Value value = new Value();
        for (Text v : values) {
            String[] valueColArr = v.toString().split(CIConst.Separator.Tab, -1);
            long minTimestamp = Long.valueOf(valueColArr[0]);
            long lastTimestamp = Long.valueOf(valueColArr[1]);
            value.minTimestamp = value.minTimestamp < minTimestamp && value.minTimestamp > 0 ? value.minTimestamp : minTimestamp;
            value.lastTimestamp = value.lastTimestamp < lastTimestamp ? lastTimestamp : value.lastTimestamp;
            value.count = value.count + Integer.valueOf(valueColArr[2]);
        }
        return value;
    }

    class Value {
        long lastTimestamp;
        long minTimestamp;
        int count;
    }

    private String getHdfsPath(String dayId, String hourId, Reducer.Context context, String tableName, String ohterPartition) {
        StringBuilder sb = new StringBuilder(CIMainParam.hiveHome).append(tableName);
        sb.append("/provider=").append(CIMainParam.provider);
        sb.append("/province=").append(CIMainParam.province);
        sb.append("/day_id=").append(dayId);
        sb.append("/hour_id=").append(hourId);
        if (!StringUtil.isEmpty(ohterPartition)) {
            sb.append("/").append(ohterPartition);
        }
        sb.append("/").append(context.getTaskAttemptID());

        return sb.toString();
    }

    private String getHourId(String keyStr) {
        String hourId = keyStr.substring(keyStr.length() - 10);
        return hourId;
    }

    private String getKeyValue(String keyStr, String outputPri) {
        return keyStr.substring(outputPri.length() + 1, keyStr.length() - 11);
    }

    private String getKeyValue(String keyStr) {
        String outputPri = keyStr.substring(0, keyStr.indexOf(SEPARATOR));
        return keyStr.substring(outputPri.length() + 1, keyStr.length() - 11);
    }

    //获取key中的s_phone_no字段（s_phone_no作为第二ge字段）
    private String getSPhoneNo(String keyStr) {
        String key = getKeyValue(keyStr);
        return key.substring(0, key.indexOf(SEPARATOR));
    }
}
