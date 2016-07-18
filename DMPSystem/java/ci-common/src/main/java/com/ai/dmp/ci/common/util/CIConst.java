package com.ai.dmp.ci.common.util;


import java.util.regex.Pattern;

/**
 * 定义内容识别相关的所有常量
 *
 */
public class CIConst {

    /**
     * 分隔符常量定义
     *
     */
    public class Separator {
        public static final String SPACE = " ";//空格
        public static final String COMMA = ",";//逗号
        public static final String SEMICOLON = ";";//分号
        public static final String WELL = "#";//#号
        public static final String AND = "&";//&符号
        public static final String EQUAL = "=";//等号
        public static final String VerticalLine = "|";//竖线
        public static final String Asterisk = "*";//星号
        public static final String Tab = "\t";//tab键
        public static final String LineBreak = "\n";//换行符
    }

    /**
     * <b>config.xml配置文件属性名称定义</b>
     * <ul>
     * <li>修改人:HCL</li>
     * <li>修改时间:2015-7-20</li>
     * <li>修改内容:添加IS_TOKEN_MATCHER_HOST_LEFT_LIKE属性</li>
     * <li></li>
     * <li>修改时间 : 2015-10-31</li>
     * <li>修改内容 : 添加用于reduce输出的标记字段</li>
     * </ul>
     *
     */
    public class Config {
        //输入数据的路径
        public final static String DATA_INPUT_SRC_PATH = "data.input.src.path";
        //输入数据字段定义 
        public final static String DATA_INPUT_FIELD = "data.input.field";
        //增强字段定义
        public final static String DATA_ENHANCE_FIELD = "data.enhance.field";
        //输入数据字段分隔符
        public final static String DATA_INPUT_FIELD_SPEARATOR = "data.input.field.separator";
        //输出数据字段分隔符
        public final static String DATA_OUTPUT_FIELD_SPEARATOR = "data.output.field.separator";
        //是否打印详细错误日志
        public final static String IS_PRINT_DETAIL_ERR_LOG = "is.print.detail.err.log";

        //是否打印详细的计数器，计数器包括：识别的goods_id数、app_tag_id数、keyword数、tag_id数、phone_no、imei、imsi、mac、idfa等 
        public final static String IS_PRINT_DETAIL_COUNTER = "is.print.detail.counter";

        //每个小时数据需要的reduce数量
        public final static String PER_HOUR_DATA_REDUCE_COUNT = "per.hour.data.reduce.count";

        //内容行为识别host匹配是否左模糊
        public final static String IS_CONT_ACTION_MATCHER_HOST_LEFT_LIKE = "is.cont.action.matcher.host.left.like";
        //用户标识识别host匹配是否左模糊
        public final static String IS_USER_FLAG_MATCHER_HOST_LEFT_LIKE = "is.user.flag.matcher.host.left.like";
        //经纬度识别host匹配是否左模糊
        public final static String IS_LOC_MATCHER_HOST_LEFT_LIKE = "is.loc.matcher.host.left.like";
        //站点识别host匹配是否左模糊
        public final static String IS_SITE_MATCHER_HOST_LEFT_LIKE = "is.site.matcher.host.left.like";
        //应用识别host匹配是否左模糊
        public final static String IS_APP_MATCHER_HOST_LEFT_LIKE = "is.app.matcher.host.left.like";

        //从mysql规则库加载是否解密
        public final static String IS_RULE_DB_ENCRYPT = "is.rule.db.encrypt";
        //从hdfs规则库加载是否解密
        public final static String IS_RULE_HDFS_ENCRYPT = "is.rule.hdfs.encrypt";
        //从哪里加载规则库    hdfs/mysql
        public final static String RULE_DB = "rule.db";
        //是否生成DPI样例数据
        public final static String IS_OUTPUT_DPI_EXAMPLE_DATA = "is.output.dpi.example.data";


        //是否将规则导入到hdfs
        public final static String IS_LOAD_RULE_FROM_MYSQL_TO_HDFS = "is.load.rule.from.mysql.to.hdfs";

        //规则库相关配置
        public final static String RULE_DB_DRIVERCLASS = "rule.db.driverclass";
        public final static String RULE_DB_URL = "rule.db.url";
        public final static String RULE_DB_USERNAME = "rule.db.username";
        public final static String RULE_DB_PASSWORD = "rule.db.password";

        //redis实例库相关配置
        public final static String REDIS_INSTANCE_DB_HOST = "redis.instance.db.host";
        public final static String REDIS_INSTANCE_DB_PORT = "redis.instance.db.port";

        //项目地配置
        public final static String GS_PROJECT_ADAPTER = "gs.project.adapter";
        public final static String MAPREDUCE_JOB_INPUTFORMAT_CLASS = "mapreduce.job.inputformat.class";
    }

    /**
     * DPI原始值字段
     */
    public class ResultColName_S {
        public static final String S_IP = "s_ip";//源IP地址
        public static final String S_AD_ID = "s_ad_id";//AD账号
        public static final String S_TIME_STAMP = "s_time_stamp";//时间戳 格式如：1400136534242
        public static final String S_URL = "s_url";//URL地址
        public static final String S_REF = "s_ref";//Refer地址
        public static final String S_UA = "s_ua";//UA
        public static final String S_DEST_IP = "s_dest_ip";//目标服务器IP
        public static final String S_COOKIE = "s_cookie";

        public static final String S_PHONE_NO = "s_phone_no";//电话号码，一般会加密
        public static final String S_IMEI = "s_imei";//移动设备标志。
        public static final String S_MEID = "s_meid";//移动设备标志。
        public static final String S_IMSI = "s_imsi";//用户的国际移动标志码
        public static final String S_MAC = "s_mac";//MAC地址
        public static final String S_IDFA = "s_idfa";//IDFA，IOS系统专用
        public static final String S_ANDROID_ID = "s_android_id";//android_id,和IDFA相似，

        public static final String S_DEST_PORT = "s_dest_port";//目标服务器端口
        public static final String S_SRC_IP = "s_src_ip";//客服端IP
        public static final String S_SRC_PORT = "s_src_port";//客服端端口
        public static final String S_IFLINK = "s_iflink";//根据get请求包中是否包含referer来判断
        public static final String S_START_TIME = "s_start_time";//业务流开始时间，格式为yyyymmddhhmmss（24小时制），如果开启中间记录模式，每条记录都填写相同的开始时间。
        public static final String S_END_TIME = "s_end_time";//业务流结束时间，格式为yyyymmddhhmmss（24小时制），如果开启中间记录模式，只在最后一条记录填写结束时间。
        public static final String S_DURATION = "s_duration";//持续时间，单位毫秒
        public static final String S_DOWN_FLOW = "s_down_flow";//下行流量，发给用户的业务字节数
        public static final String S_UP_FLOW = "s_up_flow";//上行流量，用户发出的业务字节数
        public static final String S_DOWN_PKS = "s_down_pks";//下行流量数量包，发给用户的业务数据包数量
        public static final String S_UP_PKS = "s_up_pks";//上行流量数量包，用户发出的业务数据包数量
        public static final String S_CONTENT_LEN = "s_content_len";//内容大小，content-length，协议标准字段
        public static final String S_CONTENT_TYPE = "s_content_type";//内容类型，content-type，协议标准字段
        public static final String S_HTTP_PROTYPE = "s_http_protype";//http协议类型,request method，根据请求包判断get、post字段. 电信云 6: get   5:post
        public static final String S_HTTP_STATUS = "s_http_status";//http状态
        public static final String S_RES_DELAY = "s_res_delay"; //响应时延，单位毫秒
        public static final String S_USERZONE_ID = "s_userzone_id";//用户区id
        public static final String S_SUBNET = "s_subnet";//子网号，do系统的子网和扇区标识
        public static final String S_SESSION_ID = "s_session_id";//session的id标识号
        public static final String S_RECORD_CLOSE_CAUSE = "s_record_close_cause";//记录关闭原因
        public static final String S_SERVICE_OPTION = "s_service_option";//用户所接入的cdma网络类型
        public static final String S_PRO_ID = "s_pro_id";
        public static final String S_SERVICE_TYPE = "s_service_type";//业务应用,根据端口号和协议特征等分析得出
        public static final String S_NAI = "s_nai";//网络接入标志. 网络接入标识，用户的帐号，标识用户名和归属网络，格式为：user@realm。
        public static final String S_PDSN_IP = "s_pdsn_ip";//pdsn的ipv4或ipv6地址
        public static final String S_PCF_IP = "s_pcf_ip";//pcf的ipv4或ipv6地址
        public static final String S_HA_IP = "s_ha_ip";//ha的ipv4或ipv6地址
        public static final String S_TYPE = "s_type";//广告数据或者点击数据

        public static final String S_LAC = "s_lac";
        public static final String S_CI = "s_ci";
    }

    /**
     * 内容识别识别的字段以及中间结果字段
     */
    public class ResultColName {
        public static final String START_TIMESTAMP = "start_timestamp";//开始时间的时间戳
        public static final String START_TIME = "start_time";//时间  格式：yyyyMMddkkmmss 如：20140331134916
        public static final String TOP_DOMAIN = "top_domain";//顶级域名   如：baidu.com
        public static final String COMP_DOMAIN = "comp_domain";//顶级域名   如：baike.baidu.com
        public static final String GOODS_ID = "goods_id";//商品ID   各个站点的商品ID，多个采用|分割，并且添加前缀，如jd-123
        public static final String GOODS_ACTION = "goods_action";//商品动作
        public static final String APP_ID = "app_id";//APP应用ID
        public static final String APP_ACTION = "app_action";//对APP应用的操作
        public static final String WEB_ID = "web_id";//标签ID   多个采用|分割
        public static final String WEB_ACTION = "web_action";//行为
        public static final String CONT_ID = "cont_id";//内容分类ID
        public static final String ACTION_ID = "action_id";//行为ID
        public static final String VALUE_TYPE_ID = "value_type_id";//值类型ID
        public static final String VALUE = "value";
        public static final String RECORD_TYPE = "record_type";//空为正常； 1：增加的字段（由于多个标签）
        public static final String KEYWORD = "keyword";
        public static final String LOC_ID = "loc_id";//经纬度。 格式：前缀-经度,纬度
        //        public static final String VALUE = "value";//行为值  该值和action相关。 Action=pay, value=付款(充值)金额;Action=search, value=搜索关键字
        public static final String UA_UNIFY = "ua_unify";//

        public static final String PHONE_NO = "phone_no";//电话号码
        public static final String IMEI = "imei";//imei： 国际移动设备识别码。由15位字符组成
        public static final String IMSI = "imsi";//imsi: 国际移动客户识别码，通常存储在SIM、HLR、VLR中。由15位字符组成
        public static final String MAC = "mac";//mac地址
        public static final String IDFA = "idfa";//IOS相关的标志
        public static final String ANDROID_ID = "android_id";//android_id,和IDFA相似，安卓手机专用
        public static final String COOKIE_ID = "cookie_id"; //cookie_id
        public static final String USER_NAME = "user_name"; //user_name
        public static final String EMAIL = "email"; //email

        public static final String DEVICE_MODEL = "device_model";//设备型号
        public static final String DEVICE_TYPE = "device_type";//设备类型
        public static final String DEVICE_OS = "device_os";//操作系统
        public static final String DEVICE_BROWSER = "device_browser";//浏览器

        public static final String TYPE = "type";//类型，枚举：click、ad
        public static final String LAC = "lac";
        public static final String CI = "ci";

        public static final String URL_ID = "url_id"; //url在数据库中所对应的id
        public static final String ACTION_DETAIL = "action_detail"; //action明细,json

        public static final String URI = "uri";//URL的URI部分
        public static final String REF_KW = "ref_kw"; //referer中的搜索关键字
        public static final String SITE_ID = "site_id"; //站点ID，目前仅用于搜索和旅游

        public static final String CPRO_ID = "cpro_id"; //CPROID
        public static final String BAIDU_ID = "baidu_id"; //BAIDUID

        public static final String REF_TOP_DOMAIN = "ref_top_domain"; //referer的top_domain
        public static final String REF_COMP_DOMAIN = "ref_comp_domain"; //referer的comp_domain

        public static final String TOP = "top";//该字段存储com、org、com.cn等
        public static final String TOP_DOMAIN_EXCEPT_TOP = "top_domainE_except_top";//如url为：baike.baidu.com，则该字段为baidu
        public static final String DAY_ID = "day_id";
        public static final String HOUR_ID = "hour_id";

        public static final String URL_KEY_MAP = "url_key_map";//存放URL参数Map
        public static final String REF_KEY_MAP = "ref_key_map";//存放Ref参数Map
        public static final String COOKIE_KEY_MAP = "cookie_key_map";//存放cookie参数Map
    }

    /**
     * 规则ID
     */
    public class ResultColName_RuleID {
        public static final String CLEAN_RULE_ID = "clean_rule_id";//被清洗的规则ID

        public static final String GOODS_ID_RULE_ID = "goods_id_rule_id";//商品识别的规则ID
        public static final String APP_ID_RULE_ID = "app_id_rule_id";//app识别的规则ID
        public static final String LOC_ID_RULE_ID = "loc_id_rule_id";//识别经纬度的规则ID
        public static final String SITE_ID_RULE_ID = "site_id_rule_id";//站点识别标签的规则ID
        public static final String CONT_ACTION_RULE_ID = "cont_action_rule_id";//内容行为识别标签的规则ID

        public static final String UA_UNIFY_RULE_ID = "ua_unify_rule_id";

        public static final String PHONE_NO_RULE_ID = "phone_no_rule_id";//电话号码识别规则ID
        public static final String IMEI_RULE_ID = "imei_rule_id";//imei识别规则ID
        public static final String IMSI_RULE_ID = "ims_rule_idi";//imsi识别规则ID
        public static final String MAC_RULE_ID = "mac_rule_id";//mac识别规则ID
        public static final String IDFA_RULE_ID = "idfa_rule_id";//IOS相关的标志 识别规则ID
        public static final String ANDROID_ID_RULE_ID = "android_id_rule_id";//android_id,识别规则ID
        public static final String COOKIE_ID_RULE_ID = "cookie_id_rule_id"; //cookie_id 识别规则ID
        public static final String USER_NAME_RULE_ID = "user_name_rule_id"; //user_name 识别规则ID
        public static final String EMAIL_RULE_ID = "email_rule_id"; //email识别规则ID

        public static final String DEVICE_MODEL_RULE_ID = "device_model_rule_id";//设备型号
        public static final String DEVICE_TYPE_RULE_ID = "device_type_rule_id";//设备类型
        public static final String DEVICE_OS_RULE_ID = "device_os_rule_id";//操作系统
        public static final String DEVICE_BROWSER_RULE_ID = "device_browser_rule_id";//浏览器
    }

    /**
     * 定义行为动作
     *
     * @author 小苏打
     */
    public class Action {
        public static final String CLICK = "click";//商品点击
        public static final String ADD_CART = "add_cart";//add_cart
        public static final String ADD_COLLECT = "add_collect";//商品收藏
        public static final String PAY = "pay";//付款
        public static final String ORDER = "order";//提交订单
        public static final String LOGIN = "login";//登录
        public static final String SEARCH = "search";//搜索
    }

    public class SaId {
        public static final String WEB = "web";
        public static final String SEARCH = "search";
        public static final String APP_ID = "app_id";
        public static final String GOODS_ID = "goods_id";
        public static final String LOC_ID = "loc_id";
        public static final String FLOW = "flow";
        public static final String LAC_CI = "lac_ci";
        public static final String DEVICE_MODEL = "device_model";
        public static final String DEVICE_TYPE = "device_type";
        public static final String DEVICE_OS = "device_os";
        public static final String DEVICE_BROWSER = "device_browser";
    }

    public class MFlag {
        public static final String S_IMEI = "s_imei";
        public static final String S_IMSI = "s_imsi";
        public static final String S_IDFA = "s_idfa";
        public static final String PHONE_NO = "phone_no";
        public static final String IMEI = "imei";
        public static final String IMSI = "imsi";
        public static final String MAC = "mac";
        public static final String IDFA = "idfa";
        public static final String ANDROID_ID = "android_id";
        public static final String USER_NAME = "user_name";
        public static final String EMAIL = "email";
    }

    /**
     * 用户标志类型定义
     *
     * @author 小苏打
     */
    public class UserFlagType {
        public static final String PHONE_NO = "phone_no";//电话号码
        public static final String IMEI = "imei";// 国际移动设备识别码。由15位字符组成
        public static final String IMSI = "imsi";// 国际移动客户识别码，通常存储在SIM、HLR、VLR中。由15位字符组成
        public static final String MAC = "mac";//MAC地址
        public static final String IDFA = "idfa";//IOS相关的标志
        public static final String ANDROID_ID = "android_id";
        public static final String USER_NAME = "user_name";
        public static final String EMAIL = "email";
        public static final String COOKIE_ID = "cookie_id";
    }

    /**
     * 设备类型
     *
     * @author 
     */
    public class DeviceType {
        public static final String PHONE = "phone";
        public static final String PC = "pc";
        public static final String PAD = "pad";
        public static final String TV = "tv";
        public static final String OTHER = "other";
    }

    /**
     * 运营商
     */
    public class Provider {
        public static final String LT = "lt";//联通
        public static final String YD = "yd";//移动
        public static final String DX = "dx";//电信
        public static final String DXY = "dxy";//电信云
    }

    /**
     * 省份
     */
    public class Province {
        public static final String JiangSu = "jiangsu";//江苏
    }

    /**
     * 网络类型
     */
    public class NetType {
        public static final String ADSL = "adsl";    //固网
        public static final String MOBILE = "mobile"; //移网
    }

    /**
     * 数据类型：分为广告和点击
     */
    public class Type {
        public static final String AD = "ad";
        public static final String CLICK = "click";
    }

    public class TableName {
        public static final String DMP_CI_BH = "dmp_ci_bh";
        public static final String DMP_CI_M_BH = "dmp_ci_m_bh";
        public static final String DMP_CI_TRACK_M_BH = "dmp_ci_track_m_bh";
        public static final String DMP_UC_TAGS_M_BH = "dmp_uc_tags_m_bh";
        public static final String DMP_UC_OTAGS_M_BH = "dmp_uc_otags_m_bh";
        public static final String DMP_CI_USER_M_BH = "dmp_ci_user_m_bh";
        public static final String DMP_MN_KPI_BH = "dmp_mn_kpi_bh";
    }

    public class OutputPri {
        public static final String USER = "user";
        public static final String CONT_ACTION = "cont_action";
        public static final String LOC_ID = "loc_id";
        public static final String FLOW = "flow";
        public static final String LAC_CI = "lac_ci";
        public static final String DEVICE_MODEL = "device_model";
        public static final String DEVICE_TYPE = "device_type";
        public static final String DEVICE_OS = "device_os";
        public static final String DEVICE_BROWSER = "device_browser";
        public static final String MONITOR = "monitor";

        public static final String S_IMEI = "s_imei";
        public static final String S_IMSI = "s_imsi";
        public static final String S_IDFA = "s_idfa";
        public static final String PHONE_NO = "phone_no";
        public static final String IMEI = "imei";
        public static final String IMSI = "imsi";
        public static final String MAC = "mac";
        public static final String IDFA = "idfa";
        public static final String ANDROID_ID = "android_id";
        public static final String USER_NAME = "user_name";
        public static final String EMAIL = "email";
    }

    public class ValueTypeId {
        public static final String KEYWORD_1 = "1"; //关键字
        public static final String GOODS_ID_2 = "2";  //商品ID
    }

    /**
     * 清洗规则ID
     */
    public class CleanRuleId {
        public static final String RULE_1 = "1"; //记录格式不正确
        public static final String RULE_2 = "2"; //url不合法
        public static final String RULE_3 = "3"; //phone_no为空
    }
}
