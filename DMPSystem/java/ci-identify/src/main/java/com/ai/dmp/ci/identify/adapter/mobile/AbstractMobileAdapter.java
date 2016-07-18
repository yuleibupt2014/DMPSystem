package com.ai.dmp.ci.identify.adapter.mobile;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HdfsUtil;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.adapter.AdapterUtil;
import com.ai.dmp.ci.identify.adapter.CreateTable;
import com.ai.dmp.ci.identify.adapter.CreateTablePartition;
import com.ai.dmp.ci.identify.adapter.output.AbstractMapOutput;
import com.ai.dmp.ci.identify.adapter.output.AbstractReduceOutput;
import com.ai.dmp.ci.identify.adapter.output.mobile.MobileMapOutput;
import com.ai.dmp.ci.identify.adapter.output.mobile.MobileReduceOutput;
import com.ai.dmp.ci.identify.core.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Created by yulei on 2015/10/30.
 */
public abstract class AbstractMobileAdapter extends AbstractAdapter {
    private AbstractMapOutput mapOutput = new MobileMapOutput(this);
    private AbstractReduceOutput reduceOutput = new MobileReduceOutput();

    /**
     * 创建相应的表
     *
     * @throws Exception
     */
    public void createTables(FileSystem fs) throws Exception {
        super.createTables(fs);//父类是创建公共表，比如监控表 DMP_MN_KPI_BH ——kpi value partitioned by (provider string,province string,net_type string,day_id string,hour_id ,module string)  //注意这里有net_type string
        CreateTable.createTableDmpCiBh(fs, CIConst.TableName.DMP_CI_M_BH);//内容识别明细表——输入字段与输入增强字段，例如：电信DPI原始数据。。。phone_no,imei,imsi,mac,idfa,android_id,user_name。。。partitioned by (provider string,province string,day_id string,hour_id string ,type string)
        CreateTable.createTableDmpCiTrackMBh(fs);//移动用户行为标签轨迹表 DMP_CI_TRACK_M_BH——s_phone_no app_id site_id cont_id。。。device_browser partitioned by (provider string,province string,day_id string,hour_id string type)
        CreateTable.createTableDmpUcTagsMBh(fs);//移网用户行为标签汇总表 DMP_UC_TAGS_M_BH——mix_m_uid app_id site_id cont_id action_id value value_type_id duration  last_timestamp count   partitioned by (provider string,province string,day_id string,hour_id string)
        CreateTable.createTableDmpUcOTagsMBh(fs);//移网用户其他标签汇总表 DMP_UC_OTAGS_M_BH——mix_m_uid tag_index duration last_timestamp count partitioned by (provider string,province string,day_id string,hour_id string ,sa_id string)
        CreateTable.createTableDmpCiUserMBh(fs);//创建移动用户标识汇总表 DMP_CI_USER_M_BH——s_phone_no m_id partitioned by (provider string,province string,day_id string,hour_id string ,mflag string)
    }

    /**
     * 删除已经存在的目录
     *
     * @param fs
     * @throws Exception
     */
    public void deleteHdfsPath(FileSystem fs) throws Exception {
        super.deleteHdfsPath(fs);//删除公共表的目录
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_M_BH, false, "type=click"));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_M_BH, false, "type=ad"));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_TRACK_M_BH, false, "type=click"));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_TRACK_M_BH, false, "type=ad"));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_TAGS_M_BH, false));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.LOC_ID));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.FLOW));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.LAC_CI));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.DEVICE_MODEL));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.DEVICE_TYPE));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.DEVICE_OS));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id="+CIConst.SaId.DEVICE_BROWSER));

        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IMEI));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IMSI));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IDFA));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.PHONE_NO));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IMEI));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IMSI));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.MAC));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IDFA));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.ANDROID_ID));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.USER_NAME));
        HdfsUtil.deletePath(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.EMAIL));


//        List<Path> list = new ArrayList<Path>();
//        list.add(new Path(CIMainParam.hiveHome + "ci_default/hour_id="+CIMainParam.startHourId));
//        HdfsUtil.deletePath(fs, list);
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
        super.deleteHdfsNullFile(fs);//删除公共表的空文件
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_M_BH, false, "type=click"));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_M_BH, false, "type=ad"));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_TRACK_M_BH, false, "type=click"));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_TRACK_M_BH, false, "type=ad"));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_TAGS_M_BH, false));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.LOC_ID));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.FLOW));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.LAC_CI));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.DEVICE_MODEL));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.DEVICE_TYPE));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.DEVICE_OS));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id=" + CIConst.SaId.DEVICE_BROWSER));

        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IMEI));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IMSI));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.S_IDFA));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.PHONE_NO));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IMEI));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IMSI));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.MAC));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.IDFA));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.ANDROID_ID));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.USER_NAME));
        HdfsUtil.deleteNullFile(fs, AdapterUtil.getFullPathList(CIConst.TableName.DMP_CI_USER_M_BH, false, "mflag=" + CIConst.MFlag.EMAIL));

    }

    /**
     * 创建表的分区
     *
     * @throws Exception
     */
    public void createTablePartitions() throws Exception {
        CreateTablePartition.createTablePartitionM();
    }
}
