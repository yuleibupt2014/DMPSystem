package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.CIHandlerCore;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.matcher.MatcherUtil;
import com.ai.dmp.ci.identify.monitor.MonitorHandler;
import com.ai.dmp.ci.identify.monitor.conf.MNConfig;
import com.sun.tools.internal.jxc.apt.Const;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CIMap extends Mapper<Object, Text, Text, Text> {
    private static Logger log = Logger.getLogger(CIMap.class);

    private MultipleOutputs<Text, Text> multipleOutputs;// 多文件处理类
    private CIHandlerCore handlerCore = null;// 业务处理核心类
    private AbstractAdapter adapter;// 项目地适配器
    private MonitorHandler monitorHandler;//监控处理器

    private boolean isPrintDetailErrLog = true;// 是否打印详细错误日志s

    // 是否打印详细的计数器，计数器包括：识别的goods_id数、app_tag_id数、keyword数、tag_id数、phone_no、imei、imsi、mac、idfa等
    private boolean isPrintDetailCounter = true;

    public static Configuration conf = null;// 全局配置对象

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String hourId = adapter.getHourIdByPath(filePath);

            // 业务处理：包括商品识别、应用识别、内容识别、终端标志识别等
            Result result = handlerCore.execute(value.toString());

            result.set(CIConst.ResultColName.HOUR_ID, hourId);
            result.set(CIConst.ResultColName.DAY_ID, hourId.substring(0, 8));

            //输出业务数据数据
            if (!result.ifCleaned()) {
                adapter.outputMap(result, multipleOutputs, context);
            }

            //监控以及监控输出
            if (MNConfig.isMonitor) {
                monitorHandler.handle(result, context);
            }

            CIMapCounter.printDetailCounter(context, result);
        } catch (Exception e) {
            CIMapCounter.printExceptionCounter(context);
            if (isPrintDetailErrLog) {
                log.error("处理错误！value=" + value.toString());
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 清除方法，会在每个Map任务结束之后由hadoop调用,只调用一次
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (multipleOutputs != null) {
            multipleOutputs.close();
        }
    }

    /**
     * 初始化输出文件名称以及multipleOutputs 初始化方法：会在每个Map任务开始之前由hadoop调用,只调用一次
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration(); //不能删除，在AbstractBaseMatcher使用

        try {
            // 设置程序入口参数
            String args0 = conf.get("main.args0");
            String args1 = conf.get("main.args1");
            log.info("get main.args0 : " + args0);
            log.info("get main.args1 : " + args1);

            String[] args = new String[]{args0, args1};
            CIMainParam.parseBaseArgs(args);

            adapter = AdapterUtil.getAdapter();

            MatcherUtil.loadRule();  //加载规则库 通过sql数据库找到DimAppBean等类实例的list（并赋予其初始值）

            handlerCore = new CIHandlerCore(adapter); //里面有创建数据清洗类，加载matcher匹配类，并初始化它们
            if (MNConfig.isMonitor) {
                monitorHandler = new MonitorHandler();
            }

            MatcherUtil.clearRule(); //清除加载的规则库

            isPrintDetailErrLog = Config.isPrintDetailErrLog;

        } catch (Exception e) {
            log.error("适配器配置错误！程序异常结束！");
            System.exit(1);
        }

        //  创建多文件输出类
        multipleOutputs = new MultipleOutputs<Text, Text>(context);


        //  初始化是否打印详细的计数器
        if (Config.exists(CIConst.Config.IS_PRINT_DETAIL_COUNTER)) {
            isPrintDetailCounter = Config.getBoolean(CIConst.Config.IS_PRINT_DETAIL_COUNTER);
        }
    }
}