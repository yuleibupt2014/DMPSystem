package com.ai.dmp.ci.identify.adapter.output.ads;

import com.ai.dmp.ci.identify.adapter.output.AbstractReduceOutput;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Created by yulei on 2015/10/30.
 */
public class AdslReduceOutput extends AbstractReduceOutput {

    /**
     * reduce阶段输出
     * @param key
     * @param values
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public void output(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception{

    }

}
