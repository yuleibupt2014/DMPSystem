package com.ai.dmp.ci.identify.adapter.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.HdfsFilePathFilter;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import com.ai.dmp.ci.identify.core.Result;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yulei on 2015/10/21.
 */
public class DxyMobileApapter extends AbstractMobileAdapter {
    private static Logger log = Logger.getLogger(DxyMobileApapter.class);
    SimpleDateFormat dataFormat_yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");

    public static String srcBasePath = Config.getString(CIConst.Config.DATA_INPUT_SRC_PATH);

    static {
        if (!srcBasePath.endsWith("/")) {
            srcBasePath = srcBasePath + "/" + CIMainParam.province + "/";
        } else {
            srcBasePath = srcBasePath + CIMainParam.province + "/";
        }
    }

    /**
     * 检查数据是否已经准备好
     * 默认为true，需要校验的适配器，需要重写此方法
     *
     * @param fs
     * @return true: 已准备好   false:未准备好
     */
//    public boolean checkDataReady(FileSystem fs) throws Exception {
//        String nextHourId = CIMainParam.getNextHourId();
//        String dayId = nextHourId.substring(0, 8);
//        String nextHourIdPath = srcBasePath + dayId + "/";
//
//        FileStatus[] fileStatus = fs.listStatus(new Path(nextHourIdPath));
//        for (FileStatus file : fileStatus) {
//            String filename = file.getPath().getName();
//            if (filename.indexOf(nextHourId) >= 0) {
//                return true;
//            }
//        }
//
//        return false;
//    }


    /**
     * 暂时将一天的所有数据都认为是一个小时，后续再更改
     *           如：/home/datamining/flume/yd_DPI/beijing/20150506/FixedDPI.2015050612.1430885323835
     * @return  DATA_INPUT_SRC_PATH+/+CIMainParam.province+dayId+"/"+filename
     * @throws Exception
     */
    public String getInputPath(FileSystem fs) throws Exception {
        List<String> dayIds = CIMainParam.getDayIds();
        List<String> hourIds = CIMainParam.getHourIds();

        Map<String, String> pathMap = new HashMap<String, String>();
        for (String dayId : dayIds) {
            String dayPath = srcBasePath + dayId;
            if (!fs.exists(new Path(dayPath))) {
                continue;
            }

            FileStatus[] fileStatus = fs.listStatus(new Path(dayPath));
            for (FileStatus file : fileStatus) {
                String filename = file.getPath().getName();
                String hourId = getHourIdByPath(filename);
                String fullName = dayPath + "/" + filename;
                if (hourIds.contains(hourId)) {
                    pathMap.put(fullName, null);
                }
            }
        }

        StringBuilder pathBuilder = new StringBuilder("");
        for (Map.Entry<String, String> entry : pathMap.entrySet()) {
            pathBuilder.append(entry.getKey()).append(",");
            log.info("输入目录：" + entry.getKey());
        }
        return pathBuilder.substring(0, pathBuilder.length() - 1);
    }

    /**
     * 根据文件名获取记录的Hour_id : 暂时将天的所有数据都认为是当天00点的，后续再更改
     *
     * @param filePath 如：/home/datamining/flume/yd_DPI/beijing/20150506/FixedDPI.2015050612.1430885323835
     * @return ：返回小时，如：201500612
     * @throws Exception
     */
    public String getHourIdByPath(String filePath) throws Exception {
        String hourId = filePath.split("\\.")[1];
        return hourId;
    }

    /**
     * 在对数据进行清理前，进行预处理
     *
     * @param result：原始数据
     * @return
     * @throws Exception
     */
    public void handleBeforeDC(Result result) throws Exception {
        String type = result.get(CIConst.ResultColName_S.S_TYPE);//广告数据或者点击数据
        result.set(CIConst.ResultColName.TYPE, CIConst.Type.AD.equals(type) ? CIConst.Type.AD : CIConst.Type.CLICK);
    }

    public void preHandleMatcher(Result result) throws Exception {
        //解析时间，start_timestamp
        String s_startTime = result.get(CIConst.ResultColName_S.S_START_TIME);
        if (!StringUtil.isEmpty(s_startTime)) {
            Date startDate = dataFormat_yyyyMMddHHmmss.parse(s_startTime);
            long startTimestamp = startDate.getTime();
            result.set(CIConst.ResultColName.START_TIMESTAMP, String.valueOf(startTimestamp));
        }
    }
}
