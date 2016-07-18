package com.ai.dmp.ci.identify.core.db.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.JMUtil;
import com.ai.dmp.ci.common.util.StringUtil;

public class HdfsDBUtil {
    private static Logger log = Logger.getLogger(HdfsDBUtil.class);

    //从HDFS查询数据是否需要解密
    private static boolean isRuleHdfsEncrypt = Config.getBoolean(CIConst.Config.IS_RULE_HDFS_ENCRYPT);

    private static String prePath = "dmp.ci.table.path.";
    private static String preColumn = "dmp.ci.table.column.";

    /**
     * @param conf
     * @param clazz
     * @param tableName
     * @param conds:    查询条件，目前只支持=  如:type=1,不能出现type=之类的情况,type必须为类的属性，而不是字段名称
     * @return
     * @throws Exception
     */
    public static <T> List<T> find(Configuration conf, Class<T> clazz,
                                   String tableName, String... conds) throws Exception {
        String tablePath = conf.get(prePath + tableName);  //dmp.ci.table.path.dim_ci_rule_app
        String tableColumns = conf.get(preColumn + tableName); //dmp.ci.table.column.dim_ci_rule_app
        log.info("从HDFS【" + tablePath + "】查询数据，列名:" + tableColumns);

        String[] colNames = tableColumns.split(",");//存放所有的列名
        Method[] setMethods = getSetMethod(clazz, colNames);

        //获取该表需要解密的列名
        Set<String> deColumnNames = DBUtil.getDecyptColSet(tableName);

        //查询HDFS数据
        List<String> tableDataList = readHdfsFile(conf, tablePath);
        log.info("HDFS表【" + tableName + "】记录数：" + tableDataList.size());

        //解析条件
        Map<String, String> condMap = null;
        boolean isNoCond = true;//是否存在条件查询
        if (conds != null && conds.length > 0) {
            isNoCond = false;
            condMap = new HashMap<String, String>();
            for (int i = 0; i < conds.length; i++) {
                String[] param = conds[i].split("=");
                if (param.length != 2 || !containsColumn(colNames, param[0])) {
                    throw new Exception("查询条件错误！表【" + tableName + "】字段：" + tableColumns + ",但查询条件为：" + conds[i]);
                }
                condMap.put(param[0], param[1]);
            }
        }

        //遍历结果，将每一行封装成一个对象，将每个对象添加到list列表
        List<T> resultlist = new ArrayList<T>();
        outer:
        for (int n = 0; n < tableDataList.size(); n++) {
            Object obj = clazz.newInstance();
            String lineValue = tableDataList.get(n);
            String[] value = lineValue.split(CIConst.Separator.Tab, -1);//-1保证最后为空值，也能够被分割
            for (int i = 0; i < colNames.length; i++) {
                if (deColumnNames.contains(colNames[i])) {
                    if (isRuleHdfsEncrypt && value[i] instanceof String && !StringUtil.isEmpty((String) value[i])) {
                        value[i] = JMUtil.decrypt(value[i].toString());
                    }
                }

                //如果没条件或者符合条件则将值是设置到obj对象中
                if (isNoCond || isMatchCond(colNames[i], value[i], condMap)) {
                    Object objValue = transValueType(setMethods[i], value[i]);
                    setMethods[i].invoke(obj, objValue);
                } else {
                    continue outer;
                }
            }
            resultlist.add((T) obj);
        }
        return resultlist;
    }

    /**
     * 将String类型的参数数value转换成方法需要的参数类型
     *
     * @param method
     * @param value
     * @return
     */
    private static Object transValueType(Method method, String value) {
        Object objValue = null;
        Class<?>[] classArr = method.getParameterTypes();
        if (classArr == null || classArr.length == 0) {
            return (Object) value;
        }
        String type = classArr[0].getSimpleName();
        if ("String".equals(type)) {
            objValue = (Object) value;
        } else if ("int".equals(type) || "Integer".equals(type)) {
            objValue = Integer.valueOf(value);
        } else if ("long".equals(type) || "Long".equals(type)) {
            objValue = Long.valueOf(value);
        } else if ("float".equals(type) || "Float".equals(type)) {
            objValue = Float.valueOf(value);
        } else if ("double".equals(type) || "Double".equals(type)) {
            objValue = Double.valueOf(value);
        } else if ("boolean".equals(type) || "Boolean".equals(type)) {
            objValue = Boolean.valueOf(value);
        } else if ("byte".equals(type) || "Byte".equals(type)) {
            objValue = Byte.valueOf(value);
        }
        return objValue;
    }

    /**
     * 是否满足条件
     *
     * @param colName
     * @param value
     * @param condMap
     * @return
     */
    private static boolean isMatchCond(String colName, String value, Map<String, String> condMap) {
        if (!condMap.containsKey(colName)) {//说明colName不是条件
            return true;
        }
        String condValue = condMap.get(colName);
        if (condValue != null && condValue.equals(value)) {
            return true;
        }
        return false;
    }


    /**
     * 从HDFS读取文件
     *
     * @param conf
     * @param tablePath
     * @return
     * @throws Exception
     */
    private static List<String> readHdfsFile(Configuration conf, String tablePath) throws Exception {
        FSDataInputStream in;
        BufferedReader bis;
        List<String> tableDataList = new ArrayList<String>();
        try {
            FileSystem fs = FileSystem.get(conf);
            in = fs.open(new Path(tablePath));
            bis = new BufferedReader(new InputStreamReader(in, "UTF-8"));

            String tempValue;
            while ((tempValue = bis.readLine()) != null) {
                tableDataList.add(tempValue);
            }
            return tableDataList;
        } catch (Exception e) {
            log.error("从HDFS读取数据失败，path=" + tablePath + "\n" + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 获取set方法
     *
     * @param clazz
     * @param colNames
     * @return
     */
    private static Method[] getSetMethod(Class clazz, String[] colNames) {
        Method[] methods = new Method[colNames.length];//存放set方法名
        //将set方法和对应的列名放入数组中
        for (int i = 0; i < colNames.length; i++) {
            String colName = colNames[i];
            String firstSup = colName.substring(0, 1).toUpperCase();
            String setMethodName = "set" + firstSup + colName.substring(1);
            Method method = null;
            Method[] ms = clazz.getMethods();
            for (int j = 0; j < ms.length; j++) {
                if ((ms[j].getName()).equals(setMethodName)) {
                    method = ms[j];
                }
            }
            methods[i] = method;
        }
        return methods;
    }

    /**
     * HDFS表中是否包含colName字段
     *
     * @param colNames:Hdfs表的字段
     * @param colName：需要查询的列名
     * @return
     */
    private static boolean containsColumn(String[] colNames, String colName) {
        if (colName == null || "".equals(colName)) {
            return false;
        }
        for (int i = 0; i < colNames.length; i++) {
            if (colName.equals(colNames[i])) {
                return true;
            }
        }
        return false;
    }
}
