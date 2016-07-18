package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.monitor.CIMNReduceOutput;
import com.ai.dmp.ci.identify.monitor.conf.MNConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;


public class CICombiner extends Reducer<Text, Text, Text, Text> {
    private static Logger log = Logger.getLogger(CICombiner.class);
    public static Configuration conf = null;// 全局配置对象
    private AbstractAdapter adapter;// 项目地适配器
    private CIMNReduceOutput mnOutput;//监控输出

    private boolean isPrintDetailErrLog = false;// 是否打印详细错误日志

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            if (MNConfig.isMonitor && key.toString().startsWith(CIConst.OutputPri.MONITOR)) {  //监控数据输出
                mnOutput.output(key, values, null, context);
            } else {
                adapter.outputReduce(key, values, null, context); //业务数据输出
            }
        } catch (Exception e) {
            if (isPrintDetailErrLog) {
                log.error("reduce error !\n" + e.getMessage(), e);
            }
        }
    }

    /**
     * reduce初始化:获取输出路径
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();

        try {
            // 设置程序入口参数
            String[] args = new String[]{conf.get("main.args0"), conf.get("main.args1")};
            CIMainParam.parseBaseArgs(args);

            adapter = AdapterUtil.getAdapter();
            if (MNConfig.isMonitor) {
                mnOutput = new CIMNReduceOutput();
            }
        } catch (Exception e) {
            log.error("适配器配置错误！程序异常结束！");
            log.error(e.getMessage(), e);
            System.exit(1);
        }

        //  初始化是否打印详细错误日志
        if (Config.exists(CIConst.Config.IS_PRINT_DETAIL_ERR_LOG)) {
            isPrintDetailErrLog = Config.getBoolean(CIConst.Config.IS_PRINT_DETAIL_ERR_LOG);
        }
    }
}
