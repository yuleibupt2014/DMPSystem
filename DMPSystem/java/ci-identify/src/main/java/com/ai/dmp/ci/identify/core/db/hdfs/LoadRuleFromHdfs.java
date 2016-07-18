package com.ai.dmp.ci.identify.core.db.hdfs;

import com.ai.dmp.ci.identify.core.db.util.BeanUtil;
import com.ai.dmp.ci.identify.core.db.util.DBUtil;
import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.HdfsUtil;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.List;

public class LoadRuleFromHdfs {

    private static Logger log = Logger.getLogger(LoadRuleFromHdfs.class);

    CIDaoImpl ciDao = new CIDaoImpl();

    //从HDFS加载规则库初始化
    public void execute(Configuration conf, String hiveHomePath) throws Exception {
        // 是否将规则导入到hdfs
        if (Config.isLoadRuleFromMysqlToMysql()) {
            try {
                initBeanutil();
                uploadToHdfs(conf, hiveHomePath, DimAppBean.class, ciDao.queryAllDimAppBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleAppBean.class, ciDao.queryAllDimCiRuleAppBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleBlacklistBean.class, ciDao.queryAllDimCiRuleBlacklistBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleLocBean.class, ciDao.queryAllDimCiRuleLocBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleUserFlagBean.class, ciDao.queryAllDimCiRuleUserFlagBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleSiteBean.class, ciDao.queryAllDimCiRuleSiteBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleContActionBean.class, ciDao.queryAllDimCiRuleContActionBean());
                uploadToHdfs(conf, hiveHomePath, DimCiRuleTerminalBean.class, ciDao.queryAllDimCiRuleTerminalBean());
                uploadToHdfs(conf, hiveHomePath, DimUaKwBean.class, ciDao.queryAllDimUaKwBeanBean());

                log.info("=======================上传规则库数据到HDFS完毕=======================");
            } catch (Exception e) {
                log.error("FAILED:从mysql中查询数据上传到HDFS失败！" + e.getMessage(), e);
                throw e;
            } finally {
                DBUtil.destroyConnPool();// 销毁连接池
            }
        } else {
            this.initBeanutil();

            this.setConfig(conf, hiveHomePath, DimAppBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleAppBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleBlacklistBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleLocBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleUserFlagBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleSiteBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleContActionBean.class);
            this.setConfig(conf, hiveHomePath, DimCiRuleTerminalBean.class);
            this.setConfig(conf, hiveHomePath, DimUaKwBean.class);

            log.info("从HDFS读取规则表");
        }
    }

    public void initBeanutil() {
        BeanUtil.tableCols.put(DimAppBean.class, new String[]{"appId", "contId"});
        BeanUtil.tables.put(DimAppBean.class, "dim_app");

        BeanUtil.tableCols.put(DimCiRuleAppBean.class, new String[]{"id", "host", "urlContains", "urlRegex", "uaContains", "uaRegex", "appId"});
        BeanUtil.tables.put(DimCiRuleAppBean.class, "dim_ci_rule_app");

        BeanUtil.tableCols.put(DimCiRuleSiteBean.class, new String[]{"id", "host", "siteId"});
        BeanUtil.tables.put(DimCiRuleSiteBean.class, "dim_ci_rule_site");

        BeanUtil.tableCols.put(DimCiRuleBlacklistBean.class, new String[]{"id", "blackType", "blackKey"});
        BeanUtil.tables.put(DimCiRuleBlacklistBean.class, "dim_ci_rule_blacklist");

        BeanUtil.tableCols.put(DimCiRuleLocBean.class, new String[]{"id", "host", "lngKey", "lngRegex", "latKey", "latRegex", "prefix"});
        BeanUtil.tables.put(DimCiRuleLocBean.class, "dim_ci_rule_loc");

        BeanUtil.tableCols.put(DimCiRuleUserFlagBean.class, new String[]{"id", "host", "flagType", "urlKey", "urlRegex", "cookieKey", "cookieRegex", "prefix"});
        BeanUtil.tables.put(DimCiRuleUserFlagBean.class, "dim_ci_rule_user_flag");

        BeanUtil.tableCols.put(DimCiRuleContActionBean.class, new String[]{"id", "host", "urlContains", "urlKey", "urlRegex", "refContains", "refKey", "refRegex", "contId", "actionId", "valueTypeId", "prefix"});
        BeanUtil.tables.put(DimCiRuleContActionBean.class, "dim_ci_rule_cont_action");

        BeanUtil.tableCols.put(DimCiRuleTerminalBean.class, new String[]{"id", "terminalFlag", "kw", "regex", "familyReplacement", "v1Replacement", "v2Replacement"});
        BeanUtil.tables.put(DimCiRuleTerminalBean.class, "dim_ci_rule_terminal");

        BeanUtil.tableCols.put(DimUaKwBean.class, new String[]{"id", "kw", "priority"});
        BeanUtil.tables.put(DimUaKwBean.class, "dim_ua_kw");

        log.info("设置规则库的表结构！");
    }

    /**
     * 上传表到HDFS
     * <p/>
     *
     * @param conf
     * @param hiveHomePath 需要把表上传到HDFS的路径
     * @param clazz        需要上传表对应的类
     * @param list：需要上传的数据
     * @throws Exception
     */
    public <T> void uploadToHdfs(Configuration conf, String hiveHomePath, Class<T> clazz, List<T> list) throws Exception {
        String tableName = BeanUtil.tables.get(clazz); //得到对应的表名

        this.setConfig(conf, hiveHomePath, clazz);

        String tableData = getTableData(clazz, list);

        //写入hdfs文件
        String hdfsFileName = getTableFileName(hiveHomePath, tableName);
        HdfsUtil.writeFile(FileSystem.get(conf), hdfsFileName, tableData);
        log.info("上传表【" + tableName + "】到HDFS：" + hdfsFileName + " 完成！总记录数：" + list.size());
    }

    /**
     * 查询需要上传HDFS的数据
     * 说明：还有tools也会使用此方法，修改时请注意。
     *
     * @param clazz
     * @param list
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> String getTableData(Class<T> clazz, List<T> list) throws Exception {
        String[] colNames = BeanUtil.tableCols.get(clazz);

        StringBuilder columBuilder = new StringBuilder("");
        for (int i = 0; i < colNames.length; i++) {
            if (i != 0) {
                columBuilder.append(",");
            }
            columBuilder.append(colNames[i]);
        }

        Method[] methods = new Method[colNames.length];//存放get方法名

        //构建get方法
        for (int i = 0; i < colNames.length; i++) {
            String colName = colNames[i];
            String firstSup = colName.substring(0, 1).toUpperCase();
            String setMethodName = "get" + firstSup + colName.substring(1);
            Method method = null;
            Method[] ms = clazz.getMethods();
            for (int j = 0; j < ms.length; j++) {
                if ((ms[j].getName()).equals(setMethodName)) {
                    method = ms[j];
                }
            }
            methods[i] = method;
        }

        //构建输出数据   appId  contId 换行继续
        StringBuilder valueBuilder = new StringBuilder("");
        for (int i = 0; i < list.size(); i++) {
            Object obj = list.get(i);
            for (int j = 0; j < methods.length; j++) {
                Object returnValue = methods[j].invoke(obj);
                if (j != 0) {
                    valueBuilder.append(CIConst.Separator.Tab);
                }
                if (returnValue == null) {
                    returnValue = "";
                }
                valueBuilder.append(returnValue.toString());
            }
            valueBuilder.append(CIConst.Separator.LineBreak);
        }
        return valueBuilder.toString();
    }

    private <T> void setConfig(Configuration conf, String hiveHomePath, Class<T> clazz) {
        String[] colNames = BeanUtil.tableCols.get(clazz);
        String tableName = BeanUtil.tables.get(clazz);
        String hdfsFileName = getTableFileName(hiveHomePath, tableName);

        StringBuilder columBuilder = new StringBuilder("");
        for (int i = 0; i < colNames.length; i++) {
            if (i != 0) {
                columBuilder.append(",");
            }
            columBuilder.append(colNames[i]);
        }
        //设置hadoop参数，以便Map初始化阶段能够从HDFS加载规则库
        conf.set("dmp.ci.table.path." + tableName, hdfsFileName);
        conf.set("dmp.ci.table.column." + tableName, columBuilder.toString());
    }

    private String getTableFileName(String hiveHomePath, String tableName) {
        return hiveHomePath + tableName + "/" + tableName;
    }

}
