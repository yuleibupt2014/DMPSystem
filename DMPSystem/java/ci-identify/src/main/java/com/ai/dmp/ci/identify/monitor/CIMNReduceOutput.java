package com.ai.dmp.ci.identify.monitor;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 *
 */
public class CIMNReduceOutput {
    private static Logger log = Logger.getLogger(CIMNReduceOutput.class);

    private Text keyText = new Text();
    private Text valueText = new Text();


    /**
     * 输出监控信息
     *
     * @param key
     * @param values
     * @throws Exception
     */
    public void output(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception {
        long sum = 0;
        for (Text element : values) {
            sum += Long.valueOf(element.toString());
        }
        valueText.set(sum + "");

        if (multipleOutputs == null) {  //combiner
            context.write(key, valueText);
        } else {  //reduce
            String keyStr = key.toString().substring(CIConst.OutputPri.MONITOR.length() + 1);
            keyText.set(keyStr);
            String hourId = keyStr.split("\\" + CIConst.Separator.VerticalLine, -1)[4];
            String dayId = hourId.substring(0, 8);
            String path = CIMainParam.hiveHome + CIConst.TableName.DMP_MN_KPI_BH +
                    "/provider=" + CIMainParam.provider +
                    "/province=" + CIMainParam.province +
                    "/net_type=" + CIMainParam.nettype +
                    "/day_id=" + dayId +
                    "/hour_id=" + hourId +
                    "/module=ci" +
                    "/" + context.getTaskAttemptID();
//            log.info("key:"+keyText);
//            log.info("value:"+valueText);
//            log.info("path:"+path);
            multipleOutputs.write(keyText, valueText, path);
        }
    }
}


