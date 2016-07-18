package com.ai.dmp.ci.identify.core.matcher;

import com.ai.dmp.ci.common.util.CIConst;
import com.ai.dmp.ci.common.util.StringUtil;
import com.ai.dmp.ci.identify.core.Result;
import com.ai.dmp.ci.identify.core.db.bean.BaseMatchBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yulei
 *         <p/>
 *         <ul>
 *         <li>修改人:HCL</li>
 *         <li>修改时间:2014-7-28</li>
 *         <li>修改内容:添加方法matchRef(),handleMatchRef()</li>
 *         <p/>
 *         <li>修改人:HCL</li>
 *         <li>修改时间:2014-08-21</li>
 *         <li>修改内容:添加FLAG_0,FLAG_1,FLAG_2,FLAG_3的注解</li>
 *         <ul>
 */
public abstract class BaseMatcher extends AbstractBaseMatcher implements IMatcher {
    protected static Logger log = Logger.getLogger(BaseMatcher.class);
    /**
     * 匹配失败，继续匹配
     */
    protected static final int FLAG_0 = 0;
    /**
     * 匹配成功，返回。
     */
    protected static final int FLAG_1 = 1;
    /**
     * 过滤匹配成功，返回
     */
    protected static final int FLAG_2 = 2;
    /**
     * 匹配成功，继续匹配，但host不继续左模糊
     */
    protected static final int FLAG_3 = 3;
    /**
     * 匹配成功，继续匹配
     */
    protected static final int FLAG_4 = 4;

    //缓存规则库
    private Map<String, List<BaseRuleCacheBean>> hostMap = new HashMap<String, List<BaseRuleCacheBean>>();
    protected List matchList = null;//规则库数据列表,初始化完成hostMap后，会清除该对象
    protected boolean isLeftLike = true;//host匹配是否左模糊

    /**
     * 匹配
     *
     * @param result
     */
    public boolean match(Result result) throws Exception {
        String host = result.get(CIConst.ResultColName.COMP_DOMAIN);
        String topDomain = result.get(CIConst.ResultColName.TOP_DOMAIN);

        int flag = 0;
        int length = host.split("\\.").length - topDomain.split("\\.").length;
        for (int i = 0; i <= length; i++) {
            List beanList = hostMap.get(host);
            if (beanList == null || beanList.size() == 0) {//如果根据host未找到，则获取上级域名再次查找
                if (!isLeftLike) {//如果不左模糊匹配，则直接返回
                    break;
                }
                host = host.substring(host.indexOf(".") + 1);
                continue;
            }
            for (int j = 0; j < beanList.size(); j++) {
                BaseRuleCacheBean cacheBean = (BaseRuleCacheBean) beanList.get(j);
                flag = handleMatch(cacheBean, result);
                if (flag == FLAG_0) {//匹配失败，继续匹配
                    continue;
                } else if (flag == FLAG_1) {//匹配成功，返回。
                    return true;
                } else if (flag == FLAG_2) {//过滤匹配成功，返回
                    return true;
                } else if (flag == FLAG_3) {//匹配成功，继续匹配，但host不继续左模糊
                    isLeftLike = false;
                } else if (flag == FLAG_4) {//匹配成功，继续匹配
                    continue;
                }
            }

            if (!isLeftLike) {//如果不左模糊匹配，则直接返回
                break;
            }
            host = host.substring(host.indexOf(".") + 1);
        }
        return false;
    }

    /**
     * 初始化：加载规则库数据
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void initialize() throws Exception {
        init();//初始化

        BaseMatchBean baseMatchBean = null;
        String host = null;
        List cacheBeanlist = null;
        BaseRuleCacheBean cacheBean = null;
        for (int i = 0; i < matchList.size(); i++) {
            try {
                baseMatchBean = (BaseMatchBean) matchList.get(i);
                host = baseMatchBean.getHost();
                if (StringUtils.isEmpty(host)) {
                    continue;
                }
                cacheBeanlist = hostMap.get(host);
                if (cacheBeanlist == null) {
                    cacheBeanlist = new ArrayList();
                    hostMap.put(host, cacheBeanlist);
                }
                cacheBean = transfer(baseMatchBean);//将数据库数据转为缓存数据
                if (cacheBean != null) {//如果tranfer方法里面验证未通过，则返回null
                    cacheBeanlist.add(cacheBean);
                }
            } catch (Exception e) {
                log.error("加载规则库错误！baseMatchBean=" + baseMatchBean, e);
            }
        }

        matchList = null;//清除不用对象
    }


    /**
     * 将数据库对象转换为缓存对象，如果该方法里面验证未通过，则返回null
     */
    protected abstract BaseRuleCacheBean transfer(BaseMatchBean bean) throws Exception;

    /**
     * 初始化：加载数据等
     *
     * @return
     */
    protected abstract void init() throws Exception;

    /**
     * 该方法为根据host和regex匹配上的后续处理过程
     *
     * @param result
     * @param cacheBean
     * @return result  0:匹配失败，继续匹配
     * 1:匹配成功，返回。
     * 2:过滤匹配成功，返回
     * 3:匹配成功，继续匹配，但host不继续左模糊
     */
    protected abstract int handleMatch(BaseRuleCacheBean cacheBean, Result result) throws Exception;


    /**
     * 以下三个key有且只能存在一个，否则返回null
     *
     * @param urlKey
     * @param refKey
     * @param cookieKey
     * @return
     */
    public KeyEntry getUniqueKey(String urlKey, String refKey, String cookieKey) {
        KeyEntry entry = null;
        int cnt = 0;
        String key = null;
        String resultColumn = null;
        if (!StringUtil.isEmpty(urlKey)) {
            cnt = cnt + 1;
            key = urlKey;
            resultColumn = CIConst.ResultColName_S.S_URL;
        }
        if (!StringUtil.isEmpty(refKey)) {
            cnt = cnt + 1;
            key = refKey;
            resultColumn = CIConst.ResultColName_S.S_REF;
        }
        if (!StringUtil.isEmpty(cookieKey)) {
            cnt = cnt + 1;
            key = cookieKey;
            resultColumn = CIConst.ResultColName_S.S_COOKIE;
        }

        if (cnt == 1) {
            entry = new KeyEntry();
            entry.key = key;
            entry.resultColumn = resultColumn;
        }
        return entry;
    }

    public class KeyEntry {
        public String key;
        public String resultColumn;
    }
}
