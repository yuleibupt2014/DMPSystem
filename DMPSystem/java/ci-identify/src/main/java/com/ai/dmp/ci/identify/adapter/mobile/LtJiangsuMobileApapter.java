package com.ai.dmp.ci.identify.adapter.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.FileUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.matcher.userflag.UserFlagMatcher;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yulei on 2015/10/21.
 */
public class LtJiangsuMobileApapter extends AbstractMobileAdapter {
    private static Logger log = Logger.getLogger(LtJiangsuMobileApapter.class);
    SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat dataFormat_yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");

    private static Map<String, String> sPhoneMap = new HashMap<String, String>();

    static {
        init();
    }

    public static void main(String[] args) throws Exception {
//        String hourId = new LtJiangsuMobileApapter().getHourIdByPath("/adfadf/adfadf/hour_id=2015041011/111111");
//        System.out.println("hourId = " + hourId);

    }

    /**
     * 暂时将一天的所有数据都认为是一个小时，后续再更改
     *
     * @return
     * @throws Exception
     */
    public String getInputPath(FileSystem fs) throws Exception {
        List<String> hourIds = CIMainParam.getHourIds();
        StringBuilder pathBuilder = new StringBuilder("");

        int count = Integer.MAX_VALUE;
        String maxCount = Config.getString("per.hour.data.handle.max.files");
        if (!StringUtil.isEmpty(maxCount)) {
            count = Integer.valueOf(maxCount);
            log.info("由于资源紧缺，设置每小时处理最大文件数: " + count);
        }

        for (String hourId : hourIds) {
            String monthId = hourId.substring(0, 6);
            String dayId = hourId.substring(0, 8);
            String path = srcBasePath + "month_id=" + monthId + "/day_id=" + dayId + "/hour_id=" + hourId;

            if (!fs.exists(new Path(path))) {
                continue;
            }

            FileStatus[] fileStatus = fs.listStatus(new Path(path));
            if (count > fileStatus.length) {
                count = fileStatus.length;
            }
            int[] random = AdapterUtil.randomArray(0, fileStatus.length - 1, count); //随机获取count个文件
            for (int idx : random) {
                String filename = fileStatus[idx].getPath().getName();
                String fullName = path + "/" + filename;
                if (pathBuilder.length() > 0) {
                    pathBuilder.append(",");
                }
                pathBuilder.append(fullName);
            }
        }
        log.info("输入数据：" + pathBuilder.toString());
        return pathBuilder.toString();
    }


    /**
     * 在对数据进行清理前，进行预处理
     *
     * @param result：原始数据
     * @return
     * @throws Exception
     */
    public void handleBeforeDC(Result result) throws Exception {
        String sUrl = result.get(CIConst.ResultColName_S.S_URL);
        result.set(CIConst.ResultColName.TYPE, CIConst.Type.CLICK);//设置全部数据类型为点击

        //存在如下URL： 1. http://www.baidu.com (A)(Host address)
        //              2. m.taobao.com (A)(Host address)
        if (!StringUtil.isEmpty(sUrl) && sUrl.indexOf(" ") > 0) {
            String realUrl = sUrl.split(" ")[0];
            if (!realUrl.startsWith("http://")) {
                realUrl = "http://" + realUrl;
            }
            result.set(CIConst.ResultColName_S.S_URL, realUrl);
        }
    }

    public void preHandleMatcher(Result result) throws Exception {
        //解析时间，start_timestamp
        String s_startTime = result.get(CIConst.ResultColName_S.S_START_TIME);
        if (!StringUtil.isEmpty(s_startTime) && s_startTime.length() >= 19) {
            Date startDate = dataFormat.parse(s_startTime.substring(0, 19));
            long startTimestamp = startDate.getTime();
            result.set(CIConst.ResultColName.START_TIMESTAMP, String.valueOf(startTimestamp));

            String startTime = dataFormat_yyyyMMddHHmmss.format(startDate);
            result.set(CIConst.ResultColName_S.S_START_TIME, startTime);
        }

        String s_endTime = result.get(CIConst.ResultColName_S.S_END_TIME);
        if (!StringUtil.isEmpty(s_endTime) && s_endTime.length() >= 19) {
            Date endDate = dataFormat.parse(s_endTime.substring(0, 19));
            String endTime = dataFormat_yyyyMMddHHmmss.format(endDate);
            result.set(CIConst.ResultColName_S.S_END_TIME, endTime);
        }

        //如果IDFA不合法，则过滤
        String sIdfa = result.get(CIConst.ResultColName_S.S_IDFA);
        if (!UserFlagMatcher.idfaPattern.matcher(sIdfa).find()) {
            result.set(CIConst.ResultColName_S.S_IDFA, "");
        }

        //imei标准化，江苏联通的很多imei的校验位都是有问题的
        String sImei = AdapterUtil.imeiStandardization(result.get(CIConst.ResultColName_S.S_IMEI));
        if(sImei == null ||
                sImei.startsWith("92233720368547") ||   //以下为一些错误的imei，需要过滤
                sImei.startsWith("00440015202000") ||
                sImei.startsWith("00000000000000")){
            return;
        }

        result.set(CIConst.ResultColName_S.S_IMEI,sImei);
    }

    /**
     * 是否作为样例数据输出
     *
     * @param result
     * @return
     * @throws Exception
     */
    public boolean isOutputExampleData(Result result) throws Exception {
        String sPhoneNo = result.get(CIConst.ResultColName_S.S_PHONE_NO);
        if (!sPhoneNo.isEmpty()) {
            if (sPhoneNo.startsWith("30")) {  //江苏联通只有已30开头的电话号码才作为样例数据
                return true;
            } else if (sPhoneMap.containsKey(sPhoneNo)) {//指定的phone_no
                return true;
            }
        }

        return false;
    }

    /**
     * 加载那些指定的电话号码需要输出到样例数据
     */
    private static void init() {
        try {
            InputStream in = LtJiangsuMobileApapter.class.getClassLoader().getResourceAsStream("config/lt/jiangsu/mobile/s_phone_no");
            List<String> lineList = FileUtil.readFromFile(in);
            for (String line : lineList) {
                sPhoneMap.put(line, null);
            }
            lineList = null;
        } catch (Exception e) {
            log.error("读取配置文件错误！" + e.getMessage(), e);
        }
    }

}
