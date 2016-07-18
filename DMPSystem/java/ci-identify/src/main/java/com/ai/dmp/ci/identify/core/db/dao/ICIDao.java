package com.ai.dmp.ci.identify.core.db.dao;

import com.ai.dmp.ci.identify.core.db.bean.*;

import java.util.List;

/**
 * @author yulei
 */
public interface ICIDao {

    /**
     * 查询所有的DimAppBean
     *
     * @return
     */
    public List<DimAppBean> queryAllDimAppBean() throws Exception;

    /**
     * 查询所有的DimCiRuleAppBean
     *
     * @return
     */
    public List<DimCiRuleAppBean> queryAllDimCiRuleAppBean() throws Exception;

    /**
     * 查询所有的DimCiRuleSiteBean
     *
     * @return
     */
    public List<DimCiRuleSiteBean> queryAllDimCiRuleSiteBean() throws Exception;

    /**
     * 查询所有的DimCiRuleLocBean
     *
     * @return
     */
    public List<DimCiRuleLocBean> queryAllDimCiRuleLocBean() throws Exception;

    /**
     * 根据类型查询的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryDimCiRuleUserFlagBean(String flagType) throws Exception;

    /**
     * 根据类型查询的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryAllDimCiRuleUserFlagBean() throws Exception;

    /**
     * 查询所有的DimCiRuleBlacklistBean
     *
     * @return
     */
    public List<DimCiRuleBlacklistBean> queryAllDimCiRuleBlacklistBean() throws Exception;

    /**
     * 查询所有的DimCiRuleContActionBean
     *
     * @return
     */
    public List<DimCiRuleContActionBean> queryAllDimCiRuleContActionBean() throws Exception;

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimCiRuleTerminalBean> queryAllDimCiRuleTerminalBean() throws Exception;

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimUaKwBean> queryAllDimUaKwBeanBean() throws Exception;


}
