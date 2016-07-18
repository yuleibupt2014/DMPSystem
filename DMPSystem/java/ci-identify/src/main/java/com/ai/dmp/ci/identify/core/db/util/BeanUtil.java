package com.ai.dmp.ci.identify.core.db.util;

import java.util.HashMap;
import java.util.Map;

public class BeanUtil
{

    //key：类的class对象  value：保存需要查询的字段(有顺序)
    public static Map<Class, String[]> tableCols = new HashMap<Class, String[]>();

    //key：类的class对象  value：表名
    public static Map<Class, String> tables = new HashMap<Class, String>();

}
