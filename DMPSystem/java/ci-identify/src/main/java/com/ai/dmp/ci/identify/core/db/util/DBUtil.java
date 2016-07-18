package com.ai.dmp.ci.identify.core.db.util;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.JMUtil;
import com.ai.dmp.ci.common.util.StringUtil;

/**
 * 数据库操作入口类
 *
 * @author yulei   2014-05-16
 */
public class DBUtil {
    private static Logger log = Logger.getLogger(DBUtil.class);
    //从mysql数据库查询数据是否需要解密
    private static boolean isRuleDbEncrypt = Config.getBoolean(CIConst.Config.IS_RULE_DB_ENCRYPT);

    //从hdfs规则库加载是否解密
    private static boolean isRuleHdfsEncrypt = Config.getBoolean(CIConst.Config.IS_RULE_HDFS_ENCRYPT);

    //是否将规则导入到hdfs
    private static boolean isLoadRuleFromMysqlToHdfs = Config.getBoolean(CIConst.Config.IS_LOAD_RULE_FROM_MYSQL_TO_HDFS);

    /**
     * 该方法只是适用于通过主键查询
     * 调用此类的方法，bean必须有不带参数的构造方法，bean对应到表名为bean的名字，
     * 还有bean里面的属性类型数据库里面的类型要对应
     * 如：mysql里的java.sql.Date和Java.util.Date不对应
     *
     * @param clazz  :查询对象class
     * @param sql    :sql语句
     * @param params ：sql语句使用的参数
     * @throws Exception 使用示例：
     *                   User user=(User)DBUtil.findById(User.class,"select * from user where id = ?",1001);
     * @return: 查询的某个对象
     */
    public static <T> T findById(Class<T> clazz, String sql, Object... params) throws Exception {
        List<T> list = find(clazz, sql, params);
        if (list.size() >= 1) {
            return (T) list.get(0);
        }
        return null;
    }

    /**
     * <ul>
     * <li>修改时间:2014-09-16</li>
     * <li>修改内容:添加对指定字段解密功能</li>
     * <li>修改人:HCL</li>
     * <p/>
     * <li>修改时间:2014-09-28</li>
     * <li>修改内容:修改解密逻辑,处理不存在需要解密的列的情况</li>
     * <li>修改人:HCL</li>
     * </ul>
     * 此方法可以查询该数据库里的任何表，并返回一个List对象,一系列这样的对象：DimCiRuleUserFlagBean
     *
     * @param clazz  :查询对象class
     * @param sql    :sql语句
     * @param params ：sql语句使用的参数
     * @throws Exception 使用示例：
     *                   List<User> list=DBUtil.find(User.class,"select * from user");
     * @return： list对象
     */
    public static <T> List<T> find(Class<T> clazz, String sql, Object... params) throws Exception {
        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(); //大多数连接的是这个数据库 jdbc:mysql://10.1.3.4:3306/dmp_console
            PreparedStatement ps = conn.prepareStatement(sql);
            //处理参数
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            //ResultSet元数据
            ResultSet rs = (ResultSet) ps.executeQuery();  //ResultSet 关于某个表的信息或一个查询的结果。您必须逐行访问数据行，但是您可以任何顺序访问列。
            ResultSetMetaData rsma = rs.getMetaData();  //有关 ResultSet 中列的名称和类型的信息。
            int cols = rsma.getColumnCount(); //cols获取列数
            String[] colNames = new String[cols];//存放bean中所有的列名
            Method[] methods = new Method[cols];//存放bean中所有的set方法名
            List<T> list = new ArrayList<T>();
            //将set方法和对应的列名放入数组中
            for (int i = 1; i <= cols; i++) {
                String colName = rsma.getColumnLabel(i);  //返回此列暗含的标签:flagType urlKey...
                String firstSup = colName.substring(0, 1).toUpperCase();
                String setMethodName = "set" + firstSup + colName.substring(1); //setFlagType setUrlKey...
                Method method = null;
                Method[] ms = clazz.getMethods(); //setFlagType setUrlKey
                for (int j = 0; j < ms.length; j++) {
                    if ((ms[j].getName()).equals(setMethodName)) {
                        method = ms[j];
                    }
                }
                methods[i - 1] = method;
                colNames[i - 1] = colName;
            }
            String tableName = rsma.getTableName(1); // 保存列所在的表名
            BeanUtil.tableCols.put(clazz, colNames);//放入map中，以便上传表到HDFS使用（主要是列名及顺序）
            BeanUtil.tables.put(clazz, tableName);//放入map中，以便上传表到HDFS使用
            Set<String> columnNames = getDecyptColSet(tableName);//获取该表需要解密的列名
            //遍历结果，将每一行封装成一个对象，将每个对象添加到list列表
            Object valueObj = null;
            while (rs.next()) {
                Object obj = clazz.newInstance();
                for (int i = 1; i <= cols; i++) {
                    valueObj = rs.getObject(colNames[i - 1]);
                    //如果该行的某列为null，则跳出此列的循环，操作下一列
                    if (valueObj == null || "".equals(valueObj)) {
                        continue;
                    }
                    if (columnNames.contains(colNames[i - 1])) {
                        if (isRuleDbEncrypt && valueObj instanceof String && !StringUtil.isEmpty((String) valueObj)) {
                            valueObj = JMUtil.decrypt(valueObj.toString());  //解密
                        } else if (isRuleHdfsEncrypt && isLoadRuleFromMysqlToHdfs && valueObj instanceof String && !StringUtil.isEmpty((String) valueObj)) {
                            valueObj = JMUtil.encrypt(valueObj.toString());  //加密
                        }
                    }
                    methods[i - 1].invoke(obj, valueObj);
                }
                list.add((T) obj);
            }

            return list;
        } catch (Exception e) {
            log.error("查询数据错误！" + e.getMessage(), e);
            throw e;
        } finally {
            JdbcUtil.closeConnection(conn);
        }
    }

    //获取需要解密的列名
    public static Set<String> getDecyptColSet(String tableName) {
        Set<String> columnNames = new HashSet<String>(); // 保存表中要解密的列集合

        log.info("table need to be decrypted:" + tableName);
        String columnsTemp = com.ai.dmp.ci.common.util.Config.columnToDecrypt.get(tableName);
        if (columnsTemp != null) {
            String[] columns = columnsTemp.split(",");
            log.info("columns need to be decrypted:" + columnsTemp);
            for (String column : columns) {
                columnNames.add(column);
            }
        } else {
            log.info("no columns need to be decrypted");
        }
        return columnNames;
    }

    /**
     * 通过该方法进行增删改操作
     *
     * @param sql    ：SQL语句
     * @param params ：SQL使用的参数
     * @throws Exception 使用示例：
     *                   DBUtil.cud("insert into user (name,age,birth) values(?,?,?)","cheng",20,new Date(System.currentTimeMillis()));
     *                   DBUtil.cud("update user set age=? where name=?",21,"cheng");
     *                   DBUtil.cud("delete from user where name=?","cheng");
     * @return: 返回成功操作的记录数
     */
    public static int execute(String sql, Object... params) throws Exception {
        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            int num = ps.executeUpdate();
            return num;
        } catch (Exception e) {
            log.error("操作错误！" + e.getMessage(), e);
            throw e;
        } finally {
            JdbcUtil.closeConnection(conn);
        }
    }

    /**
     * 销毁连接池
     */
    public static void destroyConnPool() {
        JdbcUtil.destroyConnPool();
    }
}