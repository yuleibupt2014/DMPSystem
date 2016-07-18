package com.ai.dmp.ci.identify.core.db.dao.impl;

import com.ai.dmp.ci.identify.core.db.util.DBUtil;
import com.ai.dmp.ci.identify.core.db.bean.*;
import com.ai.dmp.ci.identify.core.db.dao.ICIDao;
import com.ai.dmp.ci.identify.mr.CIMainParam;

import java.util.List;

/**
 * 使用该类操作数据库
 *
 * @author yulei
 */
public class CIDaoImpl implements ICIDao {


    public static void main(String[] args) throws Exception {
        String args0 = "dev,dev,mobile,2015041100,2015041201";
        String args1 = "aaaa/aaaa,dmp_online";
        CIMainParam.parseBaseArgs(new String[]{args0, args1});
        CIDaoImpl dao = new CIDaoImpl();
//        List<DimCiRuleAppBean> list = dao.queryAllDimCiRuleAppBean();
//        for (DimCiRuleAppBean bean : list) {
//            System.out.println("bean = " + bean);
//        }
//
//        List<DimCiRuleSiteBean> list1 = dao.queryAllDimCiRuleSiteBean();
//        for (DimCiRuleSiteBean bean : list1) {
//            System.out.println("bean = " + bean);
//        }
//
//        List<DimCiRuleLocBean> list2 = dao.queryAllDimCiRuleLocBean();
//        for (DimCiRuleLocBean bean : list2) {
//            System.out.println("bean = " + bean);
//        }
//
//        List<DimCiRuleUserFlagBean> list3 = dao.queryAllDimCiRuleUserFlagBean();
//        for (DimCiRuleUserFlagBean bean : list3) {
//            System.out.println("bean = " + bean);
//        }

//        List<DimCiRuleContActionBean> list4 = dao.queryAllDimCiRuleContActionBean();
//        for (DimCiRuleContActionBean bean : list4) {
////            System.out.println("bean = " + bean);
//            if (bean.getHost().equals("m.baidu.com")) {
//                System.out.println(bean.getUrlKey() + "  " + bean);
//            }
//        }
//
//
//        System.out.println(list4.size());
//        List<DimCiRuleContActionBean> list5 = dao.queryAllDimCiRuleContActionBean();
//        for (DimCiRuleContActionBean bean : list5) {
//            System.out.println("bean = " + bean);
//        }
//        dao.queryAllDimCiRuleLocBean();
//        dao.queryAllDimCiRuleUserFlagBean();
//        dao.queryDimCiRuleUserFlagBean("");
//        dao.queryAllDimCiRuleBlacklistBean();

//        List<DimCiRuleTerminalBean> list4 = dao.queryAllDimCiRuleTerminalBean();
//        for (DimCiRuleTerminalBean bean : list4) {
//            System.out.println(bean);
//        }

        List<DimUaKwBean> list4 = dao.queryAllDimUaKwBeanBean();
        for (DimUaKwBean bean : list4) {
            System.out.println(bean);
        }
    }

    /**
     * 查询所有的DimAppBean
     *
     * @return
     */
    public List<DimAppBean> queryAllDimAppBean() throws Exception {
        String sql = "select app_id as appId,cont_id as contId from dim_app where state = 1";
        return DBUtil.find(DimAppBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleAppBean
     *
     * @return
     */
    public List<DimCiRuleAppBean> queryAllDimCiRuleAppBean() throws Exception {
        String sql = "select id,host,url_contains as urlContains,url_regex as urlRegex,ua_contains as uaContains,ua_regex as uaRegex,app_id as appId from dim_ci_rule_app where state = 1 and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleAppBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleSiteBean
     *
     * @return
     */
    public List<DimCiRuleSiteBean> queryAllDimCiRuleSiteBean() throws Exception {
        String sql = "select id,host,site_id as siteId from dim_ci_rule_site where state = 1 and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleSiteBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleLocBean
     *
     * @return
     */
    public List<DimCiRuleLocBean> queryAllDimCiRuleLocBean() throws Exception {
        String sql = "select id,host,lng_key as lngKey,lng_regex as lngRegex,lat_key as latKey,lat_regex as latRegex,prefix from dim_ci_rule_loc where state = 1 and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleLocBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryAllDimCiRuleUserFlagBean() throws Exception {
        String sql = "select id,host,flag_type as flagType,url_key as urlKey,url_regex as urlRegex,cookie_key as cookieKey,cookie_regex as cookieRegex,prefix from dim_ci_rule_user_flag where state = 1 and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleUserFlagBean.class, sql);
    }


    /**
     * 根据类型查询的DimCiRuleUserFlagBean
     *
     * @return
     */
    public List<DimCiRuleUserFlagBean> queryDimCiRuleUserFlagBean(String flagType) throws Exception {
        String sql = "select id,host,flag_type as flagType,url_key as urlKey,url_regex as urlRegex,cookie_key as cookieKey,cookie_regex as cookieRegex,prefix from dim_ci_rule_user_flag where state = 1 and flag_type = '" + flagType + "' and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleUserFlagBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleBlacklistBean
     *
     * @return
     */
    public List<DimCiRuleBlacklistBean> queryAllDimCiRuleBlacklistBean() throws Exception {
        String sql = "select id,black_type as blackType,black_key as blackKey from dim_ci_rule_blacklist where state = 1 and type !='" + CIMainParam.otherNettype + "'";
        return DBUtil.find(DimCiRuleBlacklistBean.class, sql);
    }

    /**
     * 查询所有的DimCiRuleContActionBean
     *
     * @return
     */
    public List<DimCiRuleContActionBean> queryAllDimCiRuleContActionBean() throws Exception {
        //特别说明：order by url_key asc主要是为了将url_key = -1放在最前面
        String sql = "select id,host,url_contains as urlContains,url_key as urlKey,url_regex as urlRegex,ref_contains as refContains,ref_key as refKey,ref_regex as refRegex,cont_id as contId,action_id as actionId,value_type_id as valueTypeId,prefix from dim_ci_rule_cont_action where state = 1 and type !='" + CIMainParam.otherNettype + "' order by url_key asc";
        return DBUtil.find(DimCiRuleContActionBean.class, sql);
    }

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimCiRuleTerminalBean> queryAllDimCiRuleTerminalBean() throws Exception {
        String sql = "select id,terminal_flag as terminalFlag,kw,regex,family_replacement as familyReplacement,v1_replacement as v1Replacement,v2_replacement as v2Replacement from dim_ci_rule_terminal where state = 1 and type !='\" + CIMainParam.otherNettype + \"'";
        return DBUtil.find(DimCiRuleTerminalBean.class, sql);
    }

    /**
     * 查询所有DimCiRuleTerminal
     *
     * @return
     * @throws Exception
     */
    public List<DimUaKwBean> queryAllDimUaKwBeanBean() throws Exception {
        String sql = "select id,kw,priority from dim_ua_kw where state=1 order by priority asc";
        return DBUtil.find(DimUaKwBean.class, sql);
    }

}