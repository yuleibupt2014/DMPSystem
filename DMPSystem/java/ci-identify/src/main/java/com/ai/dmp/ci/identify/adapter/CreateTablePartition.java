package com.ai.dmp.ci.identify.adapter;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HdfsUtil;
import com.ai.dmp.ci.common.util.HiveUtil;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.mr.CIMain;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by yulei on 2015/11/2.
 */
public class CreateTablePartition {
    private static Logger log = Logger.getLogger(AbstractAdapter.class);

    /**
     * 创建表分区
     *
     * @throws Exception
     */
    public static void createTablePartitionM() throws Exception {
        StringBuilder sb = new StringBuilder("\nuse " + CIMainParam.hiveDatabase + ";\n");
        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_CI_M_BH, false, "type", "click", "ad"));
        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_CI_TRACK_M_BH, false, "type", "click", "ad"));
        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_UC_TAGS_M_BH, false, null));
        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_UC_OTAGS_M_BH, false, "sa_id", CIConst.SaId.LOC_ID, CIConst.SaId.FLOW, CIConst.SaId.LAC_CI,CIConst.SaId.DEVICE_MODEL,CIConst.SaId.DEVICE_TYPE,CIConst.SaId.DEVICE_OS,CIConst.SaId.DEVICE_BROWSER));
        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_CI_USER_M_BH, false,
                "mflag",CIConst.MFlag.S_IMEI,CIConst.MFlag.S_IMSI,CIConst.MFlag.S_IDFA,
                CIConst.MFlag.PHONE_NO,CIConst.MFlag.IMEI,CIConst.MFlag.IMSI,CIConst.MFlag.MAC,
                CIConst.MFlag.IDFA,CIConst.MFlag.ANDROID_ID,CIConst.MFlag.USER_NAME,CIConst.MFlag.EMAIL));

        sb.append(getCreateTablePartitionSql(CIConst.TableName.DMP_MN_KPI_BH, true, "module", "ci")); //创建监控表的分区

        if (Config.isOutputDpiExampleData) {//DPI样例数据表
            List<String> hourIds = CIMainParam.getHourIds();
            String hivesql = "alter table " + AbstractAdapter.dpiExampleDataTableName + " add if not exists partition ( ";
            for (String hourId : hourIds) {
                String sql = hivesql + "day_id="+hourId.substring(0,8)+",hour_id="+hourId+");\n";
                sb.append(sql);
            }
        }

        exeCreatePartitions(sb.toString());
    }

    /**
     * 给表创建分区
     *
     * @param tableName
     * @param hasNetTypePar : 是否包含网络类型分区
     * @param parKey
     * @param parValues
     * @throws Exception
     */
    private static String getCreateTablePartitionSql(String tableName, boolean hasNetTypePar, String parKey, String... parValues) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<String> hourIds = CIMainParam.getHourIds();
        for (String hourId : hourIds) {
            if (StringUtil.isEmpty(parKey)) {
                String hivesql = "alter table " + tableName + " add if not exists partition ( ";
                hivesql += "provider='" + CIMainParam.provider + "'";
                hivesql += ",province='" + CIMainParam.province + "'";
                if (hasNetTypePar) {
                    hivesql += ",net_type='" + CIMainParam.nettype + "'";
                }
                hivesql += ",day_id=" + hourId.substring(0, 8);
                hivesql += ",hour_id=" + hourId;
                hivesql += ");\n";
                sb.append(hivesql);
            } else {
                for (String parValue : parValues) {
                    String hivesql = "alter table " + tableName + " add if not exists partition ( ";
                    hivesql += "provider='" + CIMainParam.provider + "'";
                    hivesql += ",province='" + CIMainParam.province + "'";
                    if (hasNetTypePar) {
                        hivesql += ",net_type='" + CIMainParam.nettype + "'";
                    }
                    hivesql += ",day_id=" + hourId.substring(0, 8);
                    hivesql += ",hour_id=" + hourId;
                    hivesql += "," + parKey + "='" + parValue + "');\n";
                    sb.append(hivesql);
                }
            }
        }
        return sb.toString();
    }

    //执行sql
    private static void exeCreatePartitions(String hivesql) {
        try {
            HiveUtil.exec(hivesql);
        } catch (Exception e) {
            log.error("创建表失败：" + e.getMessage(), e);
        }
    }
}
