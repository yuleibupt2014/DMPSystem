package com.ai.dmp.ci.identify.adapter.output.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.output.AbstractMapOutput;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.ResultUtil;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by yulei on 2015/10/30.
 */
public class MobileMapOutput extends AbstractMapOutput {
    private static Logger log = Logger.getLogger(MobileMapOutput.class);

    // 输出字段分隔符
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);

    Text keyText = new Text();
    Text valueText = new Text();

    private AbstractAdapter adapter;

    public MobileMapOutput(AbstractAdapter adapter) {
        this.adapter = adapter;
    }

    /**
     * map阶段输出
     * 特别说明：reduce的输出key格式：pri\ts_phone_no\t....
     * 即s_phone_no必须为第二个字段，因为在reduce阶段或获取第二个字段为s_phone_no
     *
     * @param result
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void output(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception {
        if (StringUtil.isEmpty(result.get(CIConst.ResultColName_S.S_PHONE_NO))) {
            return;
        }
        printDetailDataToMap(result, multipleOutputs, context);//输出明细数据,直接输出，没有reduce阶段
        printUserTrackToMap(result, multipleOutputs, context);//输出用户轨迹表，直接输出，没有reduce阶段

        if (Config.isOutputDpiExampleData) { //输出DPI样例数据表
            printDpiExampleDataToMap(result, multipleOutputs, context);
        }

        printUserContActionToReduce(result, context);//输出用户行为内容，有reduce阶段
        printUserOTagsToReduce(result, context);//输出用户行为内容，有reduce阶段
        printUserFlagToRecue(result, context);//输出用户标志，有reduce阶段
    }

    /**
     * 将其他标签写入reduce，包括流量、经纬度、LAC_CI等
     *
     * @param result
     * @param context
     * @throws Exception
     */
    public void printUserOTagsToReduce(Result result, Mapper.Context context) throws Exception {
        String locId = result.get(CIConst.ResultColName.LOC_ID);
        printUserOTags(context, result, CIConst.OutputPri.LOC_ID, locId, "1");

        String upFlow = result.get(CIConst.ResultColName_S.S_UP_FLOW);
        if (!StringUtil.isEmpty(upFlow) && !"0".equals(upFlow)) {
            printUserOTags(context, result, CIConst.OutputPri.FLOW, "1", upFlow);//上行流量使用1标志
        }

        String downFlow = result.get(CIConst.ResultColName_S.S_DOWN_FLOW);
        if (!StringUtil.isEmpty(downFlow) && !"0".equals(downFlow)) {
            printUserOTags(context, result, CIConst.OutputPri.FLOW, "2", downFlow); //下行流量使用2标志
        }

        String lac = result.get(CIConst.ResultColName.LAC);
        String ci = result.get(CIConst.ResultColName.CI);
        if (!StringUtil.isEmpty(lac) && !StringUtil.isEmpty(ci)) {
            printUserOTags(context, result, CIConst.OutputPri.LAC_CI, lac + "," + ci, "1");
        }

        String deviceModel = result.get(CIConst.ResultColName.DEVICE_MODEL);
        printUserOTags(context, result, CIConst.OutputPri.DEVICE_MODEL, deviceModel, "1");
        String deviceType = result.get(CIConst.ResultColName.DEVICE_TYPE);
        printUserOTags(context, result, CIConst.OutputPri.DEVICE_TYPE, deviceType, "1");
        String deviceOs = result.get(CIConst.ResultColName.DEVICE_OS);
        printUserOTags(context, result, CIConst.OutputPri.DEVICE_OS, deviceOs, "1");
        String deviceBrowser = result.get(CIConst.ResultColName.DEVICE_BROWSER);
        printUserOTags(context, result, CIConst.OutputPri.DEVICE_BROWSER, deviceBrowser, "1");
    }

    private void printUserOTags(Mapper.Context context, Result result, String saId, String tagIndex, String count) throws Exception {
        //key: 0:saId  1:s_phone_no  2:tagIndex  3:hour_id
        //value: start_timestamp  1
        if (StringUtil.isEmpty(saId) || StringUtil.isEmpty(tagIndex) || StringUtil.isEmpty(count)) {
            return;
        }
        StringBuilder sb = new StringBuilder("");
        sb.append(saId).append(SEPARATOR);
        sb.append(result.get(CIConst.ResultColName_S.S_PHONE_NO)).append(SEPARATOR);
        sb.append(tagIndex).append(SEPARATOR);
        sb.append(result.get(CIConst.ResultColName.HOUR_ID));
        keyText.set(sb.toString());

        valueText.set(result.get(CIConst.ResultColName.START_TIMESTAMP) + SEPARATOR + count);

        context.write(keyText, valueText);
    }

    /**
     * 输出用户行为内容，有reduce阶段
     *
     * @param result
     * @param context
     * @throws Exception
     */
    public void printUserContActionToReduce(Result result, Mapper.Context context) throws Exception {
        //key: 0:cont_action  1:s_phone_no  2:app_id  3:site_id  4:cont_id  5:action_id  6:value  7:value_typeId  8:hour_id
        //value: start_timestamp  1

        String appId = result.get(CIConst.ResultColName.APP_ID);
        String siteId = result.get(CIConst.ResultColName.SITE_ID);

        if (StringUtil.isEmpty(appId) && StringUtil.isEmpty(siteId) && result.getContActionList().size() == 0) {
            return;
        }

        List<ContActionEntry> list = result.getContActionList();
        if (list.size() == 0) {
            StringBuilder sb = new StringBuilder("");
            sb.append(CIConst.OutputPri.CONT_ACTION).append(SEPARATOR);
            sb.append(result.get(CIConst.ResultColName_S.S_PHONE_NO)).append(SEPARATOR);

            sb.append(getUserTagsValue(result, "", "", "", "", false));
            sb.append(SEPARATOR).append(result.get(CIConst.ResultColName.HOUR_ID));
            keyText.set(sb.toString());

            valueText.set(result.get(CIConst.ResultColName.START_TIMESTAMP) + SEPARATOR + "1");

            context.write(keyText, valueText);
        } else {
            for (int i = 0; i < list.size(); i++) {
                StringBuilder sb = new StringBuilder("");
                sb.append(CIConst.OutputPri.CONT_ACTION).append(SEPARATOR);
                sb.append(result.get(CIConst.ResultColName_S.S_PHONE_NO)).append(SEPARATOR);

                ContActionEntry entry = list.get(i);
                String value = getUserTagsValue(result, entry.getContId(), entry.getActionId(), entry.getValue(), entry.getValueTypeId(), false);
                sb.append(value);
                sb.append(SEPARATOR).append(result.get(CIConst.ResultColName.HOUR_ID));
                keyText.set(sb.toString());

                valueText.set(result.get(CIConst.ResultColName.START_TIMESTAMP) + SEPARATOR + "1");

                context.write(keyText, valueText);
            }
        }
    }

    /**
     * 输出用户轨迹表，直接输出，没有reduce阶段
     *
     * @param result
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void printUserTrackToMap(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception {
        String appId = result.get(CIConst.ResultColName.APP_ID);
        String siteId = result.get(CIConst.ResultColName.SITE_ID);

        if (StringUtil.isEmpty(appId) && StringUtil.isEmpty(siteId) && result.getContActionList().size() == 0) {
            return;
        }

        keyText.set(result.get(CIConst.ResultColName_S.S_PHONE_NO));

        //获取写入文件路径
        String path = getHdfsPath(result, context, CIConst.TableName.DMP_CI_TRACK_M_BH);

        List<ContActionEntry> list = result.getContActionList();
        StringBuilder stringBuilder = null;
        if (list.size() == 0) {
            valueText.set(getUserTagsValue(result, "", "", "", "", true));
//            log.info("key:"+keyText);
//            log.info("value:"+valueText);
//            context.write(keyText, valueText);

            multipleOutputs.write(keyText, valueText, path);
        } else {
            for (int i = 0; i < list.size(); i++) {
                ContActionEntry entry = list.get(i);
                String value = getUserTagsValue(result, entry.getContId(), entry.getActionId(), entry.getValue(), entry.getValueTypeId(), true);
                valueText.set(value);

                multipleOutputs.write(keyText, valueText, path);
            }
        }
    }

    /**
     * @param result
     * @param contId
     * @param actionId
     * @param value
     * @param valueTypeId
     * @param isTrack     : 是否用户轨迹
     * @return
     */
    private String getUserTagsValue(Result result, String contId, String actionId, String value, String valueTypeId, boolean isTrack) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(result.get(CIConst.ResultColName.APP_ID)).append(SEPARATOR);
        stringBuilder.append(result.get(CIConst.ResultColName.SITE_ID)).append(SEPARATOR);
        stringBuilder.append(contId).append(SEPARATOR);//cont_id
        stringBuilder.append(actionId).append(SEPARATOR);//action_id
        stringBuilder.append(value).append(SEPARATOR);//value
        stringBuilder.append(valueTypeId);//value_type_id
        if (isTrack) {
            stringBuilder.append(SEPARATOR).append(result.get(CIConst.ResultColName.LOC_ID)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.LAC)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.CI)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName_S.S_START_TIME)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName_S.S_END_TIME)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.DEVICE_MODEL)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.DEVICE_TYPE)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.DEVICE_OS)).append(SEPARATOR);
            stringBuilder.append(result.get(CIConst.ResultColName.DEVICE_BROWSER));
        }
        return stringBuilder.toString();
    }

    /**
     * 输出明细数据,直接输出，没有reduce阶段
     *
     * @param result
     * @throws Exception
     */
    public void printDetailDataToMap(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception {
        String path = getHdfsPath(result, context, CIConst.TableName.DMP_CI_M_BH);
        List<String> resultList = ResultUtil.getDmpCiBhValue(result);

        for (String r : resultList) {
            keyText.set(r);
            valueText.set("");

            multipleOutputs.write(keyText, valueText, path);
        }
    }

    /**
     * 输出DPI样例数据,直接输出，没有reduce阶段
     *
     * @param result
     * @throws Exception
     */
    public void printDpiExampleDataToMap(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception {
        if (!adapter.isOutputExampleData(result)) {  //适配器判断是否需要将该数据作为样例数据输出
            return;
        }

        StringBuilder sb = new StringBuilder(CIMainParam.hiveHome).append(AbstractAdapter.dpiExampleDataTableName);
        sb.append("/day_id=").append(result.get(CIConst.ResultColName.DAY_ID));
        sb.append("/hour_id=").append(result.get(CIConst.ResultColName.HOUR_ID));
        sb.append("/").append(context.getTaskAttemptID());

        String value = ResultUtil.getValue_S(result);
        keyText.set(value);
        valueText.set("");

        multipleOutputs.write(keyText, valueText, sb.toString());
    }

    /**
     * 输出用户标志到recuce
     *
     * @param result
     * @throws Exception
     */
    public void printUserFlagToRecue(Result result, Mapper.Context context) throws Exception {
        String sPhoneNo = result.get(CIConst.ResultColName_S.S_PHONE_NO);
        if (StringUtil.isEmpty(sPhoneNo)) {
            return;
        }

        _printUserFlag(result, CIConst.ResultColName_S.S_IMEI, context);
        _printUserFlag(result, CIConst.ResultColName_S.S_IMSI, context);
        _printUserFlag(result, CIConst.ResultColName_S.S_IDFA, context);

        _printUserFlag(result, CIConst.ResultColName.PHONE_NO, context);
        _printUserFlag(result, CIConst.ResultColName.IMEI, context);
        _printUserFlag(result, CIConst.ResultColName.IMSI, context);
        _printUserFlag(result, CIConst.ResultColName.MAC, context);
        _printUserFlag(result, CIConst.ResultColName.IDFA, context);
        _printUserFlag(result, CIConst.ResultColName.ANDROID_ID, context);
        _printUserFlag(result, CIConst.ResultColName.USER_NAME, context);
        _printUserFlag(result, CIConst.ResultColName.EMAIL, context);
    }

    /**
     * 输出具体的用户标志
     *
     * @param result
     * @param column
     * @param context
     * @throws Exception
     */
    private void _printUserFlag(Result result, String column, Mapper.Context context) throws Exception {
        String flagValue = result.get(column);
        if (!StringUtil.isEmpty(flagValue)) {
            StringBuilder sb = new StringBuilder();
            sb.append(column).append(SEPARATOR);
            sb.append(result.get(CIConst.ResultColName_S.S_PHONE_NO)).append(SEPARATOR);
            sb.append(flagValue).append(SEPARATOR);
            sb.append(result.get(CIConst.ResultColName.HOUR_ID));
            keyText.set(sb.toString());
            valueText.set("");
            context.write(keyText, valueText);
        }
    }

    private String getHdfsPath(Result result, Mapper.Context context, String tableName) {
        StringBuilder sb = new StringBuilder(CIMainParam.hiveHome).append(tableName);
        sb.append("/provider=").append(CIMainParam.provider);
        sb.append("/province=").append(CIMainParam.province);
        sb.append("/day_id=").append(result.get(CIConst.ResultColName.DAY_ID));
        sb.append("/hour_id=").append(result.get(CIConst.ResultColName.HOUR_ID));
        sb.append("/type=").append(result.get(CIConst.ResultColName.TYPE));
        sb.append("/").append(context.getTaskAttemptID());

        return sb.toString();
    }
}
