package com.ai.dmp.ci.identify.core.db.dao.impl;

import com.ai.dmp.ci.identify.core.db.util.DBUtil;
import com.ai.dmp.ci.identify.core.db.util.HdfsDBUtil;
import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.ICIDao;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * 使用该类操作数据库
 *
 * @author yulei
 */
public class CIHdfsDaoImpl implements ICIDao {
    private Configuration conf = null;

    public CIHdfsDaoImpl(Configuration conf) {
        this.conf = conf;
    }

    /**
     * 查询所有的DimAppBean
     *
     * @return
     */
    public List<DimAppBean> queryAllDimAppBean() throws Exception {
        return HdfsDBUtil.find(conf, DimAppBean.class, "dim_app");
    }

    /**
     * 查询所有的DimCiRuleAppBean
     *
     * @return
     */
    public List<DimCiRuleAppBean> queryAllDimCiRuleAppBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleAppBean.class, "dim_ci_rule_app");
    }

    /**
     * 查询所有的DimCiRuleSiteBean
     *
     * @return
     */
    public List<DimCiRuleSiteBean> queryAllDimCiRuleSiteBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleSiteBean.class, "dim_ci_rule_site");
    }

    /**
     * 查询所有的DimCiRuleLocBean
     *
     * @return
     */
    public List<DimCiRuleLocBean> queryAllDimCiRuleLocBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleLocBean.class, "dim_ci_rule_loc");
    }

    /**
     * 根据类型查询的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryDimCiRuleUserFlagBean(String flagType) throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleUserFlagBean.class, "dim_ci_rule_user_flag", "flagType=" + flagType);
    }

    /**
     * 查询所有的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryAllDimCiRuleUserFlagBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleUserFlagBean.class, "dim_ci_rule_user_flag");
    }

    /**
     * 查询所有的DimCiRuleBlacklistBean
     *
     * @return
     */
    public List<DimCiRuleBlacklistBean> queryAllDimCiRuleBlacklistBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleBlacklistBean.class, "dim_ci_rule_blacklist");
    }

    /**
     * 查询所有的DimCiRuleContActionBean
     *
     * @return
     */
    public List<DimCiRuleContActionBean> queryAllDimCiRuleContActionBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleContActionBean.class, "dim_ci_rule_cont_action");
    }

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimCiRuleTerminalBean> queryAllDimCiRuleTerminalBean() throws Exception {
        return HdfsDBUtil.find(conf, DimCiRuleTerminalBean.class, "dim_ci_rule_terminal");
    }

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimUaKwBean> queryAllDimUaKwBeanBean() throws Exception {
        return HdfsDBUtil.find(conf, DimUaKwBean.class, "dim_ua_kw");
    }
}
