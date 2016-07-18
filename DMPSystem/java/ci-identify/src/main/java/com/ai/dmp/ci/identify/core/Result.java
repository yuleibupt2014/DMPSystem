package com.ai.dmp.ci.identify.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ai.dmp.ci.common.util.WordSeg;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.UrlUtil;
import com.ai.dmp.ci.identify.core.matcher.contaction.ContActionEntry;
import org.apache.commons.lang.StringUtils;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import org.apache.hadoop.mapreduce.Mapper;

public class Result {
    private Map<String, String> valueMap = new HashMap<String, String>();
    private boolean ifCleaned = false; //是否应该被清洗掉
    private Map<String, String> urlParamMap = null;//URL参数的paramMap
    private Map<String, String> refParamMap = null;//REF参数的paramMap
    private Map<String, String> cookieParamMap = null;//Cookie参数的paramMap

    private List<ContActionEntry> contActionList = new ArrayList<ContActionEntry>();//一条记录可能生成匹配内容标签
    private List<String> uaWordList = new ArrayList<String>();

    private static String DEFAULT_VALUE = "";// 默认值，此值请谨慎修改，如果修改为null，则可能造成其他地方异常

    private static String inputFieldSeparator = "\t"; //输入字段分隔符

    public Result() {
    }

    static {
        inputFieldSeparator = Config.getString(CIConst.Config.DATA_INPUT_FIELD_SPEARATOR);
        if (inputFieldSeparator.endsWith("0001")) {
            inputFieldSeparator = "\u0001";
        } else if (inputFieldSeparator.endsWith("0002")) {
            inputFieldSeparator = "\u0002";
        } else if (inputFieldSeparator.endsWith("0003")) {
            inputFieldSeparator = "\u0003";
        } else if (inputFieldSeparator.endsWith("0004")) {
            inputFieldSeparator = "\u0004";
        } else if (inputFieldSeparator.endsWith("0005")) {
            inputFieldSeparator = "\u0005";
        }
    }

    /**
     * 根据原始数据生成Result实例
     *
     * @param value
     * @return
     */
    public static Result getInstance(String value) {
        //输入数据field的分隔符
        Result result = new Result();
        String[] fieldArr = value.split(inputFieldSeparator, -1);

        int size = Config.inputColList.size();
        if (fieldArr.length < size) {
            result.setIfCleaned(true); //需要清洗掉
            result.set(CIConst.ResultColName_RuleID.CLEAN_RULE_ID, CIConst.CleanRuleId.RULE_1);//格式不正确，需要清洗掉
            return result;
        }

        List<String> inputColList = Config.inputColList;  //缓存输入字段列名
        for (int i = 0; i < size; i++) {
            result.set(inputColList.get(i), fieldArr[i]);
        }
        return result;
    }

    public List<ContActionEntry> getContActionList() {
        return contActionList;
    }

    /**
     * 调试输出
     *
     * @return
     */
    public String debugToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("=======================================================\n");
        for (Entry<String, String> entry : this.valueMap.entrySet()) {
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
            sb.append("\n");
        }
        for (ContActionEntry entry : contActionList) {
            sb.append("-----------------------------\n");
            sb.append(entry + "\n");
        }
        return sb.toString();
    }

    /**
     * 添加内容行为
     *
     * @param entry
     */
    public void addContAction(ContActionEntry entry) {
        contActionList.add(entry);
    }

    /**
     * <ul>
     * <li>创建人:小苏打</li>
     * <li>功能:设置valueMap中对应key的value</li>
     * <p/>
     * <li>修改人:HCL</li>
     * <li>修改时间:2014-08-16</li>
     * <li>修改内容:添加对email的处理</li>
     * <p/>
     * <li>修改人:HCL</li>
     * <li>修改时间:2014-08-21</li>
     * <li>修改内容:将setTagId(),setEmail()替换为setValues(),并添加对user_name的处理</li>
     * </ul>
     *
     * @param key
     * @param value
     */
    public void set(String key, String value) {

        if (value == null || StringUtils.isEmpty(value) || "null".equals(value) || "NULL".equals(value)) {
            value = DEFAULT_VALUE;
        }

        value = value.trim();
        if (CIConst.ResultColName.EMAIL.equals(key)) {
            setValues(key, value);
        } else if (CIConst.ResultColName.USER_NAME.equals(key)) {
            setValues(key, value);
        } else {
            valueMap.put(key, value);
        }
    }

    /**
     * 通过特定的分隔符替换特殊分隔符
     *
     * @param key
     * @param value
     * @param sep
     */
    public void set(String key, String value, char sep) {
        value = StringUtil.filterChar(value, sep); //过滤非法字符串
        set(key, value);
    }

    /**
     * <ul>
     * <li>创建时间:2014-09-28</li>
     * <li>创建人:HCL</li>
     * <li>功能:重设key所对应的value,而不是追加式</li>
     * </ul>
     *
     * @param key
     * @param value
     */
    public void reset(String key, String value) {
        valueMap.put(key, value);
    }

    public String get(String key) {
        String value = valueMap.get(key);
        if (value == null || "null".equals(value) || "NULL".equals(value)) {
            value = DEFAULT_VALUE;
        }
        return value;
    }

    /**
     * 将key所对应的value追加式保存
     * <ul>
     * <li>创建时间:2014-08-21</li>
     * <li>创建人:HCL</li>
     * <li>操作: 删除了setTagId()与setEmail(),添加通用方法setValues()</li>
     * </ul>
     *
     * @param value
     * @param key   CIConst.ResultColName中的列名
     */
    private void setValues(final String key, String value) {
        String oldValue = valueMap.get(key);
        if (oldValue != null && !"".equals(oldValue)) {
            if (value != null && !"".equals(value)) {
                String[] oldValues = oldValue.split("\\" + CIConst.Separator.VerticalLine);
                String[] values = value.split("\\" + CIConst.Separator.VerticalLine);
                List<String> valueList = new ArrayList<String>();
                for (int i = 0; i < oldValues.length; i++) {
                    valueList.add(oldValues[i].toLowerCase());
                }

                for (int i = 0; i < values.length; i++) {
                    if (!valueList.contains(values[i].toLowerCase())) {
                        valueList.add(values[i]);
                    }
                }

                StringBuilder valueBuilder = new StringBuilder("");
                for (int i = 0; i < valueList.size(); i++) {
                    valueBuilder.append(valueList.get(i)).append(CIConst.Separator.VerticalLine);
                }
                String tmpValue = valueBuilder.toString();
                if (tmpValue.endsWith(CIConst.Separator.VerticalLine)) {
                    tmpValue = tmpValue.substring(0, tmpValue.length() - 1);
                }
                valueMap.put(key, tmpValue);
            }
        } else {
            valueMap.put(key, value);
        }
    }

    public void setIfCleaned(boolean ifCleaned) {
        this.ifCleaned = ifCleaned;
    }

    public boolean ifCleaned() {
        return ifCleaned;
    }

    /**
     * 获取UA的分词列表
     * @return
     */
    public List<String> getUaWordList() {
        String sUa = this.get(CIConst.ResultColName_S.S_UA);
        if(uaWordList.size() > 0 || StringUtil.isEmpty(sUa)){
            return uaWordList;
        }

        uaWordList = WordSeg.wordSeg(sUa); //分词
        return uaWordList;
    }

    public void setUaWordList(List<String> uaWordList) {
        this.uaWordList = uaWordList;
    }

    /**
     * 根据参数名称获取参数值
     *
     * @param paramName        ：参数名称
     * @param resultColumnName ：从哪个字段获取，目前只能从url/ref/cookie中获取
     * @return
     * @throws Exception
     */
    public String getParamValue(String paramName, String resultColumnName) throws Exception {
        if (CIConst.ResultColName_S.S_URL.equals(resultColumnName)) {
            return getUrlParamValue(paramName);
        } else if (CIConst.ResultColName_S.S_REF.equals(resultColumnName)) {
            return getRefParamValue(paramName);
        } else if (CIConst.ResultColName_S.S_COOKIE.equals(resultColumnName)) {
            return getCookieParamValue(paramName);
        }
        return null;
    }

    /**
     * 根据URL的参数获取参数值
     *
     * @param paramName
     * @return
     * @throws Exception
     */
    public String getUrlParamValue(String paramName) throws Exception {
        String urlStr = this.get(CIConst.ResultColName_S.S_URL);
        if (urlParamMap == null) {
            urlParamMap = UrlUtil.parseUrlParam(urlStr);
        }
        return urlParamMap.get(paramName);
    }

    /**
     * 获取REF的参数
     *
     * @param paramName
     * @return
     * @throws Exception
     */
    public String getRefParamValue(String paramName) throws Exception {
        String refStr = this.get(CIConst.ResultColName_S.S_REF);
        if (refParamMap == null) {
            refParamMap = UrlUtil.parseUrlParam(refStr);
        }
        return refParamMap.get(paramName);
    }

    public String getCookieParamValue(String paramName) throws Exception {
        String cookieStr = this.get(CIConst.ResultColName_S.S_COOKIE);
        if (cookieParamMap == null) {
            cookieParamMap = UrlUtil.parseCookieParam(cookieStr);
        }
        return cookieParamMap.get(paramName);
    }
}