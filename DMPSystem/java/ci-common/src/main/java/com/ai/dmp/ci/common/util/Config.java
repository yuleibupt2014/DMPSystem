package com.ai.dmp.ci.common.util;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class Config {

    public static void main(String[] args) {
        List<String> tableList = Config.getTables();
        for (int i = 0; i < tableList.size(); i++) {
            System.out.println(tableList.get(i));
        }
    }

    private static Logger log = Logger.getLogger(Config.class);
    /**
     * table list that want to be exported
     */
    public static List<String> exportTableArr = null;
    /**
     * key set: table NO
     */
    public static Set<String> tableKeys = null;
    /**
     * map : tableNo-tableName
     */
    public static Map<String, String> mapTable = null;
    /**
     * <ul>
     * record column_names in table_name to encrypt</li>
     * </ul>
     */
    public static Map<String, String> mapColumn = null;

    /**
     * map : columns to decrypt
     */
    public static Map<String, String> columnToDecrypt = null;

    static {
        try {
            mapTable = new HashMap<String, String>();
            mapColumn = new HashMap<String, String>();
            columnToDecrypt = new HashMap<String, String>();
            exportTableArr = new ArrayList<String>();

            initTable();
            initColumn();
            switchColumns();
        } catch (Exception e) {
            log.error("初始化失败！程序退出！" + e.getMessage());
            System.exit(1);
        }

    }

    /**
     * 获取所有表名
     *
     * @return
     */
    public static List<String> getTables() {
        List<String> tableList = new ArrayList<String>();
        Set<String> keys = mapTable.keySet();
        for (String key : keys) {
            tableList.add(mapTable.get(key));
        }
        return tableList;
    }

    /**
     * parse the file of table.properties to get the table list that want to be exported
     */
    private static void initTable() {
        try {
            Properties props = new Properties();
            InputStream in = new BufferedInputStream(Config.class.getClassLoader().getResourceAsStream("config/table.properties"));
            props.load(in);
            tableKeys = props.stringPropertyNames();

            for (String key : tableKeys) {
                mapTable.put(key, props.getProperty(key));
            }
            in.close();
        } catch (Exception e) {
            log.error("initial error！" + e.getMessage() + "\n", e);
        }
    }

    /**
     * parse the file of column.properties to get the table.column list that want to be encrypted
     */
    private static void initColumn() {
        try {
            Properties props = new Properties();
            InputStream in = new BufferedInputStream(Config.class.getClassLoader().getResourceAsStream("config/column.properties"));
            props.load(in);
            Set<String> columnKeys = props.stringPropertyNames();

            for (String key : columnKeys) {
                mapColumn.put(key, props.getProperty(key));
            }
            in.close();
        } catch (Exception e) {
            log.error("initial error！" + e.getMessage() + "\n", e);
        }
    }

    /**
     * show help information
     *
     * @return String
     */
    public static String help() {
        StringBuilder help = new StringBuilder("\n\n");
        help.append("Usage: java -jar <xxx.jar> <output> <table1_NO> [<table2_NO> <...>]\n\n");
        help.append("--select the number of the table you want to encrypt:\n");
        for (int i = 1; i <= mapTable.size(); i++) {
            help.append("\t" + i + " : " + mapTable.get(i + "") + "\n");
        }
        help.append("--modify the file of /src/main/resources/config/column.properties to define the column you want to encrypt.\n");
        return help.toString();
    }

    /**
     * switch column name's format: <br/>
     * eg: ref_regex -> refRegex
     */
    public static void switchColumns() {
        Set<Map.Entry<String, String>> entries = mapColumn.entrySet();
        String key = null;
        String value = null;
        StringBuilder sb = null;
        for (Map.Entry<String, String> entry : entries) {
            key = entry.getKey().toString();
            value = entry.getValue().toString();
            sb = new StringBuilder(value);

            int index = sb.indexOf("_");
            while (index >= 0) {
                char digit = sb.charAt(index + 1);
                digit = (char) (digit - 32);
                sb.replace(index, index + 2, digit + "");
                index = sb.indexOf("_", index);
            }
            value = sb.toString();
            columnToDecrypt.put(key, value);
        }
    }
}
