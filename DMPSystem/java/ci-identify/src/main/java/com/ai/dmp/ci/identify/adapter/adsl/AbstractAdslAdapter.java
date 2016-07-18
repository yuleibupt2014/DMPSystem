package com.ai.dmp.ci.identify.adapter.adsl;

import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.output.*;
import com.ai.dmp.ci.identify.adapter.output.ads.AdslMapOutput;
import com.ai.dmp.ci.identify.adapter.output.ads.AdslReduceOutput;
import com.ai.dmp.ci.identify.core.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Created by yulei on 2015/10/30.
 */
public abstract class AbstractAdslAdapter extends AbstractAdapter {
    private AbstractMapOutput mapOutput = new AdslMapOutput();
    private AbstractReduceOutput reduceOutput = new AdslReduceOutput();

    /**
     * 创建相应的表
     *
     * @throws Exception
     */
    public void createTables(FileSystem fs) throws Exception {
    }


    /**
     * 删除已经存在的目录
     *
     * @param fs
     * @throws Exception
     */
    public void deleteHdfsPath(FileSystem fs) throws Exception {

    }

    /**
     * map阶段输出
     *
     * @param result
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void outputMap(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception {
        mapOutput.output(result, multipleOutputs, context);
    }

    /**
     * reduce阶段输出
     *
     * @param key
     * @param values
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void outputReduce(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception {
        reduceOutput.output(key, values, multipleOutputs, context);
    }

    /**
     * 删除空文件
     *
     * @param fs
     * @throws Exception
     */
    public void deleteHdfsNullFile(FileSystem fs) throws Exception {

    }

    /**
     * 创建表的分区
     *
     * @throws Exception
     */
    public void createTablePartitions() throws Exception {

    }
}
