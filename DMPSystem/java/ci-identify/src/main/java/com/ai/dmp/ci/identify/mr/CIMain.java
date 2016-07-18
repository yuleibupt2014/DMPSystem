package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HdfsUtil;
import com.ai.dmp.ci.common.util.HiveUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.hdfs.LoadRuleFromHdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.util.*;

public class CIMain {
    private static Logger log = Logger.getLogger(CIMain.class);

    private AbstractAdapter adapter = null;
    FileSystem fs = null;

    /**
     * @param args args[0]=provider,province,nettype,start_hour_id,end_hour_id
     *             eg: dxy,sh,mobile,2015041110,2015041211
     *             args[1]=hiveHome，hiveDataBase
     *             args[2]=hadoop_param1=value1,hadoop_param2=value2        //可能没有args[2]
     *             <p/>
     *             eg:  hadoop jar dmp-ci-0.0.1-SNAPSHOT.jar lt,jiangsu,mobile,2015011501,2015011501 /user/hive/warehouse/dmptest_user_dir/chengxf/,chengxf
     *             eg:  hadoop jar dmp-ci-0.0.1-SNAPSHOT.jar lt,jiangsu,mobile,2015011501,2015011501 /user/hive/warehouse/dmptest_user_dir/chengxf/,chengxf hadoop_param1=value1,hadoop_param2=value2
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new CIMain(args).run(args);
    }

    public CIMain(String[] args) {
        try {
            CIMainParam.parseBaseArgs(args);
            adapter = AdapterUtil.getAdapter();
        } catch (Exception e) {
            log.fatal("解析参数错误！程序异常退出！" + e.getMessage(), e);
            System.exit(1);
        }
    }

    public boolean run(String[] args) throws Exception {
        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "company-dmp-ci");
        fs = FileSystem.get(conf);

        //等待数据，直到准备好
        waitDataReady(fs);

        adapter.createTables(fs); //创建各种相关的表！！！代码在AbstractMobileAdapter里面

        //删除已存在的输出路径
        adapter.deleteHdfsPath(fs);

        //解析hadoop相关参数
        CIMainParam.setHadoopArgs(args, job);
        setInputFormatClass(job);//设置文件类型格式

        //获取输入路径或输入文件
        String inputs = adapter.getInputPath(fs); //inputs=/home/datamining/flume/yd_DPI/beijing/20150506/FixedDPI.2015050612.1430885323835,...后面以天和文件名分割
        FileInputFormat.addInputPaths(job, inputs);

        // 设置map输出给reduce的流路径
        FileOutputFormat.setOutputPath(job, new Path(CIMainParam.hiveHome + "ci_default/"+System.currentTimeMillis()));

        // 设置job的配置参数
        job.setJarByClass(CIMain.class);
        job.setMapperClass(CIMap.class);
        job.setReducerClass(CIReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setCombinerClass(CICombiner.class);//设置combiner
        job.setPartitionerClass(CIPartitioner.class);//设置partitioner
//        job.setNumReduceTasks(0);
        job.setNumReduceTasks(getReduceCount());

        // 如果启用从hdfs加载规则库，则此处将规则库上传到HDFS，在每个Map中从HDFS加载数据库  将所有的bean以换行分隔的形式写入到hdfs里面，每列为bean的字段
        if (Config.isRuleDbHdfs() || Config.isLoadRuleFromMysqlToMysql()) {
            new LoadRuleFromHdfs().execute(job.getConfiguration(), CIMainParam.hiveHome);
        }

        // 提交job
        boolean isSuc = job.waitForCompletion(true);
        log.info("Job finished!");

        if (isSuc) {
            // 删除空文件
            adapter.deleteHdfsNullFile(fs);

            //创建表的分区
            adapter.createTablePartitions();
        }
        log.info("dmp-ci finished!!!!!!!");
        return isSuc;
    }

    private int getReduceCount() throws Exception {
        return CIMainParam.getHourIds().size() * Config.getInt(CIConst.Config.PER_HOUR_DATA_REDUCE_COUNT);
    }

    /**
     * 等待数据就绪，直到准备好
     *
     * @param fs
     * @throws Exception
     */
    private void waitDataReady(FileSystem fs) throws Exception {
        while (true) {
            boolean isReady = adapter.checkDataReady(fs);    //在AbstractAdapter返回true，需要校验的适配器，需要重写此方法
            if (isReady) {
                return;
            } else {
                log.warn(CIMainParam.endHourId + " data is not ready! wait 1 minute......");
                Thread.sleep(60 * 1000);//休眠一分钟
            }
        }
    }

    /**
     * 设置输入文件类型： SequenceFileInputFormat/TextInputFormat  等 (dxy.mobile/config.xml文件中配置的是SequenceFileInputFormat，其它为TextInputFormat)
     *
     * @param job
     */
    public void setInputFormatClass(Job job) throws Exception {
        //默认为文本类型格式，所以什么也不用做即可。
        String className = Config.getString(CIConst.Config.MAPREDUCE_JOB_INPUTFORMAT_CLASS);
        if (!StringUtil.isEmpty(className)) {
            try {
                Class clazz = (Class) Class.forName(className);
                job.setInputFormatClass(clazz);
            } catch (Exception e) {
                log.error("设置输入文件格式不正确！程序退出执行！className=" + className + "\n" + e.getMessage(), e);
                System.exit(1);
            }
        }
    }
}
