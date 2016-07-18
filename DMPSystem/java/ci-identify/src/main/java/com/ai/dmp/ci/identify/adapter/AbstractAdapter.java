package com.ai.dmp.ci.identify.adapter;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HdfsUtil;
import com.ai.dmp.ci.common.util.HiveUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.ResultUtil;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import com.ai.dmp.ci.identify.core.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yulei on 2015/10/21.
 */
public abstract class AbstractAdapter {
    private static Logger log = Logger.getLogger(AbstractAdapter.class);
    public static String srcBasePath = Config.getString(CIConst.Config.DATA_INPUT_SRC_PATH);

    //dpi样例数据表
    public static String dpiExampleDataTableName = "dmp_dpi_" + CIMainParam.provider + "_ " + CIMainParam.province + "_" + CIMainParam.nettype + "_bh";

    static {
        if (!srcBasePath.endsWith("/")) {
            srcBasePath = srcBasePath + "/";
        }
    }

    /**
     * 检查数据是否已经准备好
     * 默认为true，需要校验的适配器，需要重写此方法
     *
     * @param fs
     * @return true: 已准备好   false:未准备好
     */
    public boolean checkDataReady(FileSystem fs) throws Exception {
        return true;
    }

    /**
     * 创建相应的表
     *
     * @throws Exception
     */
    public void createTables(FileSystem fs) throws Exception {
        CreateTable.createTableDmpMnKpiBh(fs);

        if (Config.isOutputDpiExampleData) {
            CreateTable.createTableDpiExampleData(fs);//创建DPI样例数据表
        }
    }

    /**
     * 删除已经存在的目录
     *
     * @param fs
     * @throws Exception
     */
    public void deleteHdfsPath(FileSystem fs) throws Exception {
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_MN_KPI_BH, true, "module=ci"));

        if (Config.isOutputDpiExampleData) {
            //删除DPI样例数据表的数据
            HdfsUtil.deletePath(fs, AdapterUtil.getDpiExampleDataFullPathList(dpiExampleDataTableName));
        }
    }

    /**
     * 根据开始时间和结束时间获取输入路径
     *
     * @return
     */

    public abstract String getInputPath(FileSystem fs) throws Exception;

    /**
     * 根据该记录所在文件名获取记录的Hour_id
     *
     * @param filePath
     * @return
     * @throws Exception
     */
    public String getHourIdByPath(String filePath) throws Exception {
        int endIdx = filePath.lastIndexOf("/");
        String hourId = filePath.substring(endIdx - 10, endIdx);
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

    }

    /**
     * 对原始数据进行预处理,在进行识别之前
     *
     * @param result：原始数据
     * @return
     * @throws Exception
     */
    public abstract void preHandleMatcher(Result result) throws Exception;

    /**
     * map阶段输出
     *
     * @param result
     * @param multipleOutputs
     * @param context
     * @throws Exception
     */
    public abstract void outputMap(Result result, MultipleOutputs<Text, Text> multipleOutputs, Mapper.Context context) throws Exception;


    /**
     * 是否作为样例数据输出
     *
     * @param result
     * @return
     * @throws Exception
     */
    public boolean isOutputExampleData(Result result) throws Exception {
        return false;
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
    public abstract void outputReduce(Text key, Iterable<Text> values, MultipleOutputs<Text, Text> multipleOutputs, Reducer.Context context) throws Exception;

    /**
     * 删除空文件
     *
     * @param fs
     * @throws Exception
     */
    public void deleteHdfsNullFile(FileSystem fs) throws Exception {
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_MN_KPI_BH, true, "module=ci"));

        if (Config.isOutputDpiExampleData) {
            //删除DPI样例数据表的数据
            HdfsUtil.deleteNullFile(fs, AdapterUtil.getDpiExampleDataFullPathList(dpiExampleDataTableName));
        }
    }

    /**
     * 创建表的分区
     *
     * @throws Exception
     */
    public abstract void createTablePartitions() throws Exception;

}
