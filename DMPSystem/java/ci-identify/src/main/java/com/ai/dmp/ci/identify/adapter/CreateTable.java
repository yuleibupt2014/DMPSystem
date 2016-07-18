package com.ai.dmp.ci.identify.adapter;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HiveUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMain;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by yulei on 2015/10/2.
 */
public class CreateTable {
    private static Logger log = Logger.getLogger(AbstractAdapter.class);
    // 输出字段分隔符
    private static String SEPARATOR = Config.getString(CIConst.Config.DATA_OUTPUT_FIELD_SPEARATOR);

    //创建内容识别明细表 ——输入字段与输入增强字段，例如：phone_no,imei,imsi,mac,idfa,android_id,user_name。。。
    public static void createTableDmpCiBh(FileSystem fs, String tableName) throws Exception {
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));

        for (int i = 0; i < Config.inputColList.size(); i++) {  //inputColList为缓存的输入字段列名
            hivesql.append("        " + Config.inputColList.get(i) + " string,\n");
        }
        for (int i = 0; i < Config.enhanceColList.size(); i++) {  //enhanceColList为缓存的输入增强字段列名
            if (i == Config.enhanceColList.size() - 1) {
                hivesql.append("        " + Config.enhanceColList.get(i) + " string\n");
            } else {
                hivesql.append("        " + Config.enhanceColList.get(i) + " string,\n");
            }
        }
        hivesql.append(getParCreateTableSql("type"));
        hivesql.append(getLastCreateTableSQL(tableName));

        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 创建用户轨迹表（移动）
     * * @throws Exception
     */
    public static void createTableDmpCiTrackMBh(FileSystem fs) throws Exception {
        String tableName = CIConst.TableName.DMP_CI_TRACK_M_BH;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));
        hivesql.append("        s_phone_no string,\n");
        hivesql.append("        app_id string,\n");
        hivesql.append("        site_id string,\n");
        hivesql.append("        cont_id string,\n");
        hivesql.append("        action_id string,\n");
        hivesql.append("        value string,\n");
        hivesql.append("        value_type_id string,\n");
        hivesql.append("        loc_id string,\n");
        hivesql.append("        lac string,\n");
        hivesql.append("        ci string,\n");
        hivesql.append("        start_time string,\n");
        hivesql.append("        end_time string,\n");
        hivesql.append("        device_model string,\n");
        hivesql.append("        device_type string,\n");
        hivesql.append("        device_os string,\n");
        hivesql.append("        device_browser string\n");
        hivesql.append(getParCreateTableSql("type"));

        hivesql.append(getLastCreateTableSQL(tableName));
        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 创建移网用户行为标签汇总表（移网）
     * * @throws Exception
     */
    public static void createTableDmpUcTagsMBh(FileSystem fs) throws Exception {
        String tableName = CIConst.TableName.DMP_UC_TAGS_M_BH;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));
        hivesql.append("        mix_m_uid string,\n");
        hivesql.append("        app_id string,\n");
        hivesql.append("        site_id string,\n");
        hivesql.append("        cont_id string,\n");
        hivesql.append("        action_id string,\n");
        hivesql.append("        value string,\n");
        hivesql.append("        value_type_id string,\n");
        hivesql.append("        duration string,\n");
        hivesql.append("        last_timestamp string,\n");
        hivesql.append("        count int\n");
        hivesql.append(getParCreateTableSql());
        hivesql.append(getLastCreateTableSQL(tableName));
        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 创建移网用户其他标签汇总表（移动）
     * * @throws Exception
     */
    public static void createTableDmpUcOTagsMBh(FileSystem fs) throws Exception {
        String tableName = CIConst.TableName.DMP_UC_OTAGS_M_BH;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));
        hivesql.append("        mix_m_uid string,\n");
        hivesql.append("        tag_index string,\n");
        hivesql.append("        duration string,\n");
        hivesql.append("        last_timestamp string,\n");
        hivesql.append("        count int\n");
        hivesql.append(getParCreateTableSql("sa_id"));
        hivesql.append(getLastCreateTableSQL(tableName));
        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 移网用户标识汇总表（移动）
     * * @throws Exception
     */
    public static void createTableDmpCiUserMBh(FileSystem fs) throws Exception {
        String tableName = CIConst.TableName.DMP_CI_USER_M_BH;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));
        hivesql.append("        s_phone_no string,\n");
        hivesql.append("        m_id string\n");
        hivesql.append(getParCreateTableSql("mflag"));
        hivesql.append(getLastCreateTableSQL(tableName));
        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 创建按小时监控数据表
     * * @throws Exception
     */
    public static void createTableDmpMnKpiBh(FileSystem fs) throws Exception {
        String tableName = CIConst.TableName.DMP_MN_KPI_BH;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));//获取创建表SQL的前半部分
        hivesql.append("        kpi string,\n");
        hivesql.append("        value double\n");
        hivesql.append("    )\n");    //以下是表的分区
        hivesql.append("    partitioned by (provider string,province string,net_type string,day_id string,hour_id string,module string)");

        hivesql.append(getLastCreateTableSQL(tableName));//获取创建表SQL的后半部分
        exeCreateTable(hivesql.toString(), tableName);
    }

    /**
     * 创建DPI样例数据表
     * * @throws Exception
     */
    public static void createTableDpiExampleData(FileSystem fs) throws Exception {
        String tableName = AbstractAdapter.dpiExampleDataTableName;
        String tablePath = CIMainParam.hiveHome + tableName;
        if (fs.exists(new Path(tablePath))) {
            return;
        }

        StringBuilder hivesql = new StringBuilder();
        hivesql.append(getPreCreateTableSQL(tableName));
        List<String> inputColList = Config.inputColList;
        for (int i = 0; i < inputColList.size(); i++) {
            if (i == inputColList.size() - 1) {
                hivesql.append("        "+inputColList.get(i)+" string\n");
            } else {
                hivesql.append("        "+inputColList.get(i)+" string,\n");
            }
        }
        hivesql.append("    )\n");
        hivesql.append("    partitioned by (day_id string,hour_id string)\n");
        hivesql.append(getLastCreateTableSQL(tableName));
        exeCreateTable(hivesql.toString(), tableName);
    }

    private static String getParCreateTableSql(String... pars) {
        StringBuilder hivesql = new StringBuilder();
        hivesql.append("    )\n");
        hivesql.append("    partitioned by (provider string,province string,day_id string,hour_id string");
        for (int i = 0; i < pars.length; i++) {
            String par = pars[i];
            if (i == pars.length - 1) {
                hivesql.append(",").append(par).append(" string");
            } else {
                hivesql.append(",").append(par).append(" string,");
            }
        }
        hivesql.append(")\n");
        return hivesql.toString();
    }

    //获取创建表SQL的前半部分
    private static String getPreCreateTableSQL(String tableName) {
        StringBuilder hivesql = new StringBuilder("");
        hivesql.append("    use " + CIMainParam.hiveDatabase + ";\n");
        hivesql.append("    create table if not exists " + tableName + "\n");
        hivesql.append("    ( \n");

        return hivesql.toString();
    }

    //获取创建表SQL的后半部分
    private static String getLastCreateTableSQL(String tableName) {
        StringBuilder hivesql = new StringBuilder("");
        hivesql.append("    row format delimited fields terminated by '" + SEPARATOR + "' \n");
        hivesql.append("    stored as TEXTFILE \n");
        hivesql.append("    location '" + CIMainParam.hiveHome + tableName + "' \n");

        return hivesql.toString();
    }

    //执行sql
    private static void exeCreateTable(String hivesql, String tableName) {
        try {
            log.info("开始创建表：" + tableName);
            HiveUtil.exec(hivesql);
        } catch (Exception e) {
            log.error("创建表失败：" + tableName + " \n" + e.getMessage(), e);
        }
    }
}
