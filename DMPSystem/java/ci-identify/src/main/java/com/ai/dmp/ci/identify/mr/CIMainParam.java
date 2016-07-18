package com.ai.dmp.ci.identify.mr;

import com.ai.dmp.ci.common.util.CIConst;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class CIMainParam {
    private static Logger log = Logger.getLogger(CIMainParam.class);

    public static String provider = null; //运营商  dxy/dx/yd/lt
    public static String province = null; //省份   beijing/shanghai
    public static String nettype = null; //网络类型   adsl/mobile
    public static String startHourId = null;//开始时间
    public static String endHourId = null;//结束时间
    public static String hiveHome = null;//hive的Home目录
    public static String hiveDatabase = null;//hive 数据库
    public static int hourCount = 0;//总过多少个小时

    public static boolean isMobileNetType = false;//是否移动网络
    public static String otherNettype = null;

    private static SimpleDateFormat dataFormat_yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
    private static SimpleDateFormat dataFormat_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception{
        initJsltTest();
        System.out.println(getNextHourId());
    }

    public static void initJsltTest() throws Exception {
        String args0 = "lt,jiangsu,mobile,20150411,20150412";
        String args1 = "aaaa/aaaa,dmp_online";
        CIMainParam.parseBaseArgs(new String[]{args0, args1});
    }

    /**
     * 加密工具初始化，请不要删除
     *
     * @throws Exception
     */
    public static void initJMTools() throws Exception {
        String args0 = "lt,jiangsu,mobile,2015041100,2015041200";
        String args1 = "aaaa/aaaa,dmp_online";
        CIMainParam.parseBaseArgs(new String[]{args0, args1});
    }

    /**
     * 解析程序参数
     *
     * @param args
     */
    public static void parseBaseArgs(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            log.info("args[" + i + "]: " + args[i]);
        }

        if (args.length < 2) {
            throw new Exception("参数不足！程序退出！");
        }

        String[] arg0s = args[0].split(",");
        if (arg0s.length < 5) {
            throw new Exception("参数不正确，程序退出！args[0]=provider,province,nettype,start_hour_id,end_hour_id");
        }

        if (Integer.valueOf(arg0s[3]) > Integer.valueOf(arg0s[4])) {
            throw new Exception("开始时间不能大约结束时间！程序退出！");
        }
        provider = arg0s[0];
        province = arg0s[1];
        nettype = arg0s[2];
        startHourId = arg0s[3];
        endHourId = arg0s[4];
        hourCount = getHourIds().size();

        String[] args1 = args[1].split(",");
        if (args1.length != 2) {
            throw new Exception("没有传入hive主目录或hive数据库！");
        }
        if (args1[0].endsWith("/")) {
            hiveHome = args1[0];
        } else {
            hiveHome = args1[0] + "/";
        }
        hiveDatabase = args1[1];

        if (CIConst.NetType.MOBILE.equals(nettype)) {
            isMobileNetType = true;
            otherNettype = CIConst.NetType.ADSL;
        } else {
            otherNettype = CIConst.NetType.MOBILE;
        }
    }

    /**
     * 解析hadoop参数
     *
     * @param args
     * @throws Exception
     */
    public static void setHadoopArgs(String[] args, Job job) throws Exception {
        job.getConfiguration().set("main.args0", args[0]);//设置参数，在Map和Reduce阶段获取并解析
        job.getConfiguration().set("main.args1", args[1]);//设置参数，在Map和Reduce阶段获取并解析
        log.info("set main.args0: "+args[0]);
        log.info("set main.args1: "+args[1]);

        if (args.length == 3) {
            String[] hadoopParams = args[2].split(",");
            for (int i = 0; i < hadoopParams.length; i++) {
                if (hadoopParams[i].isEmpty())
                    continue;
                if (hadoopParams[i].split("=")[0].isEmpty())
                    continue;
                String[] params = hadoopParams[i].split("=");
                job.getConfiguration().set(params[0], params[1]);   //hadoop_param1=value1,hadoop_param2=value2
                log.info("set hadoop param: " + params[0] + "=" + params[1]);
            }

            //取消推测执行,map和reduce都要取消
            job.getConfiguration().set("mapreduce.reduce.speculative", "false");
            log.info("set hadoop param: mapreduce.reduce.speculative=false");

            job.getConfiguration().set("mapreduce.map.speculative", "false");
            log.info("set hadoop param: mapreduce.map.speculative=false");
        }
    }

    /**
     * 根据开始时间和结束时间计算所有的小时
     *
     * @return
     * @throws Exception
     */
    public static List<String> getHourIds() throws Exception {
        List<String> hourIdList = new ArrayList<String>();
        if (startHourId.equals(endHourId)) {
            hourIdList.add(startHourId);
            return hourIdList;
        }

        Date startDate = dataFormat_yyyyMMddHH.parse(startHourId);
        Date endDate = dataFormat_yyyyMMddHH.parse(endHourId);
        Calendar cal = Calendar.getInstance();

        cal.setTime(startDate);
        while (cal.getTimeInMillis() <= endDate.getTime()) {
            hourIdList.add(dataFormat_yyyyMMddHH.format(cal.getTime()));
            cal.add(Calendar.HOUR, 1);
        }
        return hourIdList;
    }

    /**
     * 根据开始时间和结束时间计算所有的天
     *
     * @return
     * @throws Exception
     */
    public static List<String> getDayIds() throws Exception {
        String startDayId = startHourId.substring(0, 8);
        String endDayId = endHourId.substring(0, 8);

        List<String> dayIdList = new ArrayList<String>();
        if (startDayId.equals(endDayId)) {
            dayIdList.add(startDayId);
            return dayIdList;
        }

        Date startDate = dataFormat_yyyyMMdd.parse(startDayId);
        Date endDate = dataFormat_yyyyMMdd.parse(endDayId);
        Calendar cal = Calendar.getInstance();

        cal.setTime(startDate);
        while (cal.getTimeInMillis() <= endDate.getTime()) {
            dayIdList.add(dataFormat_yyyyMMdd.format(cal.getTime()));
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }
        return dayIdList;
    }

    /**
     * 获取endHourId的下一个小时
     *
     * @return
     */
    public static String getNextHourId() throws Exception {
        Date endDate = dataFormat_yyyyMMddHH.parse(endHourId);
        Calendar cal = Calendar.getInstance();

        cal.setTime(endDate);
        cal.add(Calendar.HOUR, 1);
        String netHourId = dataFormat_yyyyMMddHH.format(cal.getTime());
        return netHourId;
    }
}
