package com.ai.dmp.ci.identify.adapter.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.FileUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
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
public class YdZhejiangMobileApapter extends AbstractMobileAdapter {
    private static Logger log = Logger.getLogger(YdZhejiangMobileApapter.class);
    SimpleDateFormat dataFormat_yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 暂时将一天的所有数据都认为是一个小时，后续再更改
     *
     * @return
     * @throws Exception
     */
    public String getInputPath(FileSystem fs) throws Exception {
        List<String> hourIds = CIMainParam.getHourIds();
        StringBuilder pathBuilder = new StringBuilder("");

        for (String hourId : hourIds) {
            String path = srcBasePath +  "day_id="+hourId.substring(0,8)+"/hour_id="+hourId+"/";
            if (!fs.exists(new Path(path))) {
                continue;
            }

            pathBuilder.append(path);
        }

        String inputPath = null;
        if(pathBuilder.length()>0){
            inputPath = pathBuilder.substring(0,pathBuilder.length()-1);
        }
        log.info("输入数据：" + inputPath);
        return inputPath;
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
        if(!StringUtil.isEmpty(sUrl)){
            result.set(CIConst.ResultColName_S.S_URL, "http://"+sUrl);
        }

        result.set(CIConst.ResultColName.TYPE, CIConst.Type.CLICK);//设置全部数据类型为点击
    }

    public void preHandleMatcher(Result result) throws Exception {
        //解析时间，start_timestamp
        String s_startTime = result.get(CIConst.ResultColName_S.S_START_TIME);
        if (!StringUtil.isEmpty(s_startTime) && s_startTime.length() ==10) {
            Date startDate = new Date(Long.valueOf(s_startTime+"000"));
            String startTime = dataFormat_yyyyMMddHHmmss.format(startDate);
            result.set(CIConst.ResultColName_S.S_START_TIME, startTime);
            result.set(CIConst.ResultColName.START_TIMESTAMP, String.valueOf(startDate.getTime()));
        }

        String s_endTime = result.get(CIConst.ResultColName_S.S_END_TIME);
        if (!StringUtil.isEmpty(s_endTime) && s_endTime.length() ==10) {
            Date endDate =new Date(Long.valueOf(s_endTime+"000"));
            String endTime = dataFormat_yyyyMMddHHmmss.format(endDate);
            result.set(CIConst.ResultColName_S.S_END_TIME, endTime);
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

}
