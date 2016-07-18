package com.ai.dmp.ci.identify.dataclean;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.adapter.AbstractAdapter;
import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.mr.CIMapCounter;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.net.URL;

/**
 * Created by yulei on 2015/10/22.
 */
public class DataCleaning {
    private static Logger log = Logger.getLogger(DataCleaning.class);
    private AbstractAdapter adapter = null;//适配器

    public DataCleaning(AbstractAdapter adapter) {
        this.adapter = adapter;
    }

    /**
     * 数据清洗
     *
     * @param value
     * @return
     */
    public Result cleaning(String value) throws Exception {
        Result result = Result.getInstance(value);

        adapter.handleBeforeDC(result);

        if (result.ifCleaned()) {
            return result;
        }

        //判断URL是否合法
        filterByURL(result);
        if (result.ifCleaned()) {
            return result;
        }

        //判断电话号码是否为空  如果电话号码为空，需要清洗掉
        filterNotNull(result, CIConst.ResultColName_S.S_PHONE_NO, CIConst.CleanRuleId.RULE_3);
        if (result.ifCleaned()) {
            return result;
        }

        return result;
    }

    private void filterNotNull(Result result, String column, String ruleId) {
        if (StringUtil.isEmpty(result.get(column))) {
            result.setIfCleaned(true); //需要清洗掉
            result.set(CIConst.ResultColName_RuleID.CLEAN_RULE_ID, ruleId);
        }
    }

    /**
     * 根据URL判断是否合法
     *
     * @param result
     */
    public void filterByURL(Result result) {
        try {
            String host = null;
            int port = 0;
            String urlStr = result.get(CIConst.ResultColName_S.S_URL);
            if (StringUtils.isEmpty(urlStr)) {
                result.setIfCleaned(true); //需要清洗掉
                result.set(CIConst.ResultColName_RuleID.CLEAN_RULE_ID, CIConst.CleanRuleId.RULE_2);//URL为空，需要过滤掉
                return;
            } else if (!urlStr.startsWith("http://")) {
                result.setIfCleaned(true); //需要清洗掉
                result.set(CIConst.ResultColName_RuleID.CLEAN_RULE_ID, CIConst.CleanRuleId.RULE_2);//URL格式不合法
                return;
            }
            URL url = new URL(urlStr);
            host = url.getHost();
            port = url.getPort();

            if (StringUtils.isEmpty(host)) {
                throw new RuntimeException("host不能为空！ ");
            } else if (!host.contains(".")) {
                throw new RuntimeException("host至少要包含一个'.'");
            } else if (!CharUtils.isAsciiAlphanumeric(host.charAt(0))) {
                throw new RuntimeException("host不能以字母和数字以外的字符开头");
            } else if (!StringUtils.containsOnly(host.toLowerCase(), "0123456789-_.:abcdefghijklmnopqrstuvwxyz")) {
                throw new RuntimeException("host不能包含'.','-','_',':'以外的特殊符号,host:" + host);
            } else if (port > 65535 || port < -1) {
                throw new RuntimeException("端口号的值应该在[1-65535]");
            }
        } catch (Exception e) {
            result.setIfCleaned(true); //需要清洗掉
            result.set(CIConst.ResultColName_RuleID.CLEAN_RULE_ID, CIConst.CleanRuleId.RULE_2);//URL格式不合法
            if (Config.isPrintDetailErrLog) {
                log.warn(e.getMessage(), e);
            }
        }
    }
}
