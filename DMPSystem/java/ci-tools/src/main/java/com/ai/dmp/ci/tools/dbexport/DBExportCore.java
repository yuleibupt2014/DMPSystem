package com.ai.dmp.ci.tools.dbexport;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashSet;
import java.util.Set;

import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.impl.CIDaoImpl;
import com.ai.dmp.ci.identify.core.db.hdfs.LoadRuleFromHdfs;
import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.util.Config;
import com.ai.dmp.ci.common.util.JMUtil;
import com.ai.dmp.ci.identify.core.db.util.JdbcUtil;

public class DBExportCore {
    private static Logger log = Logger.getLogger(DBExportCore.class);

    private LoadRuleFromHdfs loadHdfs;
    private String outFileName;

    public DBExportCore(String outFileName) {
        this.outFileName = outFileName;
        loadHdfs = new LoadRuleFromHdfs();
        loadHdfs.initBeanutil();
    }

    public void execute() throws Exception {
        if (Config.exportTableArr == null || Config.exportTableArr.size() == 0) {
            log.info("没有需要导出的表！");
            log.info("No table selected to be exported！");
            return;
        }
        int length = Config.exportTableArr.size();

        for (int i = 0; i < length; i++) {
            export(Config.exportTableArr.get(i));
        }

//        exportHdfsFile();
    }

    private void exportHdfsFile() throws Exception {
        CIDaoImpl ciDao = new CIDaoImpl();
        writeFile("dim_ci_rule_app", loadHdfs.getTableData(DimCiRuleAppBean.class, ciDao.queryAllDimCiRuleAppBean()));
        writeFile("dim_ci_rule_blacklist", loadHdfs.getTableData(DimCiRuleBlacklistBean.class, ciDao.queryAllDimCiRuleBlacklistBean()));
        writeFile("dim_ci_rule_loc", loadHdfs.getTableData(DimCiRuleLocBean.class, ciDao.queryAllDimCiRuleLocBean()));
        writeFile("dim_ci_rule_user_flag", loadHdfs.getTableData(DimCiRuleUserFlagBean.class, ciDao.queryAllDimCiRuleUserFlagBean()));
        writeFile("dim_ci_rule_site", loadHdfs.getTableData(DimCiRuleSiteBean.class, ciDao.queryAllDimCiRuleSiteBean()));
        writeFile("dim_ci_rule_blacklist", loadHdfs.getTableData(DimCiRuleContActionBean.class, ciDao.queryAllDimCiRuleContActionBean()));
    }

    private void writeFile(String tableName, String tableData) throws Exception {
//        String path = "hdfsfile/";
//        File pathFile = new File(path);
//        if (!pathFile.exists()) {
//            pathFile.mkdir();
//        }

        String path = "hdfsfile/"+tableName+"/";
        File pathFile = new File(path);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }

        File file = new File(path + tableName);
        FileOutputStream out = new FileOutputStream(file);
        out.write(tableData.getBytes());
        out.close();
    }

    public void export(String tablename) {
        Connection conn = null;
        Writer out = null;
        try {
            // get the columns' name that need to be encrypted
            String[] columns = Config.mapColumn.get(tablename).split(",");
            Set<String> colSet = new HashSet<String>();
            for (String column : columns) {
                colSet.add(column.trim());
                log.info(tablename + "." + column);
            }


            conn = JdbcUtil.getConnection();
            String sql = "select * from " + tablename;
            PreparedStatement ps = conn.prepareStatement(sql);

            // ResultSet元数据
            ResultSet rs = (ResultSet) ps.executeQuery();
            ResultSetMetaData rsma = rs.getMetaData();
            int cols = rsma.getColumnCount();

            String[] colNames = new String[cols];// 存放所有的列名
            Object[] colValues = new Object[cols];// 存放值

            // 将set方法和对应的列名放入数组中
            for (int i = 1; i <= cols; i++) {
                String colName = rsma.getColumnLabel(i);
                colNames[i - 1] = colName;
                log.info("ColumnLabel." + colName);
            }

            try {
                out = new OutputStreamWriter(new FileOutputStream(outFileName, true), "UTF-8");
            } catch (FileNotFoundException e) {
                log.error("初始化输出流错误!");
                log.error("Initial outputstream error！ outFileName=" + outFileName + "\n" + e.getMessage(), e);
                throw e;
            }
            WriteFile rf = new WriteFile(out, tablename, colNames);
            rf.writeHead();
            // 遍历结果，将每一行封装成一个对象，将每个对象添加到list列表
            Object valueObj = null;
            int cnt = 0;
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    try {
                        valueObj = rs.getObject(colNames[i - 1]);
                    } catch (Exception ee) {
                        valueObj = "";
                    }
                    if (valueObj instanceof String && valueObj != null && !"".equals(valueObj)) {
                        if (colSet.contains(colNames[i - 1])) {
                            valueObj = JMUtil.encrypt(valueObj.toString());
                        }
                    }
                    colValues[i - 1] = valueObj;
                }
                rf.write(colValues);// 写入数据
                cnt++;
                if (cnt % 100 == 0) {
                    log.info("table[" + tablename + "] export finished " + cnt);
                }
            }
            log.info("table[" + tablename + "] export finished 100%,cnt=" + cnt);
        } catch (Exception e) {
            log.error("查询数据错误!");
            log.error("Select data error！" + e.getMessage(), e);
        } finally {
            JdbcUtil.closeConnection(conn);
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("关闭输出流错误!");
                    log.error("Close OutputStreamWriter error！" + e.getMessage(), e);
                }
            }
        }
    }
}
