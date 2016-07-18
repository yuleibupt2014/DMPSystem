package com.ai.dmp.ci.identify.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.util.CIConst;

public class Config extends AbstractConfig {
    private static Logger log = Logger.getLogger(Config.class);

    public static List<String> inputColList = new ArrayList<String>();//缓存输入字段列名 见config.xml中的data.input.field:s_imsi=0,s_phone_no=1,s_meid=2,s_nai=3....
    public static List<String> enhanceColList = new ArrayList<String>();//缓存增强字段列名 见default/config.xml中的data.enhance.field：phone_no,imei,imsi,mac,idfa,android_id,user_name,email...

    public static boolean isPrintDetailErrLog = true;//是否打印详细日志

    public static boolean isOutputDpiExampleData = false;//是否输出DPI样例数据

    static {
        initInputist(CIConst.Config.DATA_INPUT_FIELD);//将data.input.field封装成List
        initOutputList(CIConst.Config.DATA_ENHANCE_FIELD);//将data.output.field封装成List

        init();
    }

    /**
     * 将属性值封装成List
     *
     * @param paramString
     */
    private static void initInputist(String paramString) {
        String value = PS.getString(paramString);
        if (value != null && !"".equals(value)) {
            String[] valueArr = value.split(CIConst.Separator.COMMA);
            String[] tmpArr = null;
            for (int i = 0; i < valueArr.length; i++) {
                tmpArr = valueArr[i].split(CIConst.Separator.EQUAL);
                if (tmpArr.length != 2) {
                    log.error("配置不正确，程序退出！");
                    System.exit(0);
                }
                inputColList.add(tmpArr[0]);
            }
        }
    }

    //判断是否启用从HDFS加载规则库
    public static boolean isRuleDbHdfs() {
        String ruleDb = getString(CIConst.Config.RULE_DB);
        if ("mysql".equals(ruleDb)) {
            return false;
        } else {
            return true;
        }
    }

    //是否将规则导入到hdfs
    public static boolean isLoadRuleFromMysqlToMysql() {
        return getBoolean(CIConst.Config.IS_LOAD_RULE_FROM_MYSQL_TO_HDFS);
    }

    /**
     * 将属性值封装成List
     *
     * @param paramString
     */
    private static void initOutputList(String paramString) {
        String value = PS.getString(paramString);
        if (value != null && !"".equals(value)) {
            String[] valueArr = value.split(CIConst.Separator.COMMA);
            for (int i = 0; i < valueArr.length; i++) {
                enhanceColList.add(valueArr[i]);
            }
        }
    }

    public static void init() {
        //  初始化是否打印详细错误日志
        if (Config.exists(CIConst.Config.IS_PRINT_DETAIL_ERR_LOG)) {
            isPrintDetailErrLog = Config.getBoolean(CIConst.Config.IS_PRINT_DETAIL_ERR_LOG);
        }

        //  是否输出DPI的样例数据
        if (Config.exists(CIConst.Config.IS_OUTPUT_DPI_EXAMPLE_DATA)) {
            isOutputDpiExampleData = Config.getBoolean(CIConst.Config.IS_OUTPUT_DPI_EXAMPLE_DATA);
        }
    }

    public static void main(String[] args) {

    }
}