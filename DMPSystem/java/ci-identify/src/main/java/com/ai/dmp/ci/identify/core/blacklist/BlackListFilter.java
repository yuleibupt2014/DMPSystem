package com.ai.dmp.ci.identify.core.blacklist;

import java.util.ArrayList;
import java.util.List;

import com.ai.dmp.ci.identify.core.db.bean.DimCiRuleBlacklistBean;
import com.ai.dmp.ci.identify.core.matcher.AbstractBaseMatcher;
import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.core.Result;


/**
 * <ul>
 * <li>创建时间:2014-09-28</li>
 * <li>作用:过滤内容识别中出现的黑名单关键字</li>
 * </ul>
 *
 * @author yulei
 */
public class BlackListFilter extends AbstractBaseMatcher {
    protected static Logger log = Logger.getLogger(BlackListFilter.class);
    private static BlackListFilter blackListFilter = new BlackListFilter();

    protected List<BlackListCacheBean> blackList = new ArrayList<BlackListCacheBean>(); //黑名单列表

    private BlackListFilter(){}

    public static BlackListFilter getInstance(){
        return blackListFilter;
    }

    /**
     * 过滤黑名单中所有类型
     *
     * @param result 日志记录
     */
    public void filter(Result result) {
        if (blackList != null) {
            for (BlackListCacheBean blackBean : blackList) {
                this.filter(result, blackBean);
            }
        }
    }

    /**
     * 过滤日志中的指定字段
     *
     * @param result        日志对象
     * @param blackBean <ul>要过滤的字段:<li>CIConst.ResultColName.USER_NAME</li><li>CIConst.ResultColName.COOKIE_ID</li><ul>
     */
    public void filter(Result result, BlackListCacheBean blackBean) {
        try {
            String blackType = blackBean.getBlackType();

            if (StringUtil.isEmpty(result.get(blackType))) {
                return;
            }

            //过滤用户名黑名单
            String[] needCheckKeys = result.get(blackType).split("\\" + CIConst.Separator.VerticalLine);
            //黑名单list
            List<String> keys = blackBean.getBlackKeyList();

            //过滤黑名单
            //如果是cookie_id和user_name，则需要将前缀去掉，再过滤。
            boolean flag = false;//是否需要重新给result对象复制
            if (blackType.equals(CIConst.ResultColName.COOKIE_ID) || blackType.equals(CIConst.ResultColName.USER_NAME)) {
                for (int i = 0; i < needCheckKeys.length; i++) {
                    int index = -1;
                    index = needCheckKeys[i].indexOf("_");
                    if (index >= 0) {
                        if (keys.contains(needCheckKeys[i].substring(index + 1))) {
                            needCheckKeys[i] = "";
                            flag = true;
                        }
                    }
                }
            } else {//其他
                for (int i = 0; i < needCheckKeys.length; i++) {
                    if (keys.contains(needCheckKeys[i])) {
                        needCheckKeys[i] = "";
                        flag = true;
                    }
                }
            }

            if (flag) {
                //构造过滤后的字符串
                StringBuilder sb = new StringBuilder("");
                for (int i = 0; i < needCheckKeys.length; i++) {
                    if (!needCheckKeys.equals(""))
                        sb.append(needCheckKeys[i] + "|");
                }
                if (sb.length() > 0)
                    sb = sb.deleteCharAt(sb.length() - 1);
                result.reset(blackType, sb.toString());
                result.reset(blackType+"_rule_id", "");
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 获取黑名单
     *
     * @return 黑名单列表(只有一条记录)
     * @throws Exception
     */
    public void initialize() throws Exception {
        List<DimCiRuleBlacklistBean> blackListBeans = ciDao.queryAllDimCiRuleBlacklistBean();
        for (DimCiRuleBlacklistBean bean : blackListBeans) {
            if(StringUtil.isEmpty(bean.getBlackType()) || StringUtil.isEmpty(bean.getBlackKey())) {
                continue;
            }

            List<String> blackKeyList = new ArrayList<String>();
            String[] blackKeys = bean.getBlackKey().split("\\" + CIConst.Separator.VerticalLine);
            for(String key : blackKeys){
                blackKeyList.add(key);
            }
            BlackListCacheBean cacheBean = new BlackListCacheBean();
            cacheBean.setBlackType(bean.getBlackType());
            cacheBean.setBlackKeyList(blackKeyList);
            blackList.add(cacheBean);
        }
    }
}
