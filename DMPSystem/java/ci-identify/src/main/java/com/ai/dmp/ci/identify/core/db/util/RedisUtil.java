package com.ai.dmp.ci.identify.core.db.util;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.CIConst;

public class RedisUtil {

private static Logger log = Logger.getLogger(RedisUtil.class);
	
	private static String host;
	private static String port;
	
	private static Jedis jedis;

	static {
		init();//初始化，包括读取配置文件等
	}
	
	/**
	 * 初始化：读取配置文件以及创建连接池
	 */
	private static void init(){
		try {
			host =  Config.getString(CIConst.Config.REDIS_INSTANCE_DB_HOST);
			port = Config.getString(CIConst.Config.REDIS_INSTANCE_DB_PORT);
			jedis = new Jedis(host,Integer.parseInt(port));
		} catch (Exception e) {
			log.error("获取redis连接错误！"+e.getMessage(),e);
		}
	}
	
	public static Jedis getJdiesInstance() throws Exception{
		if(jedis == null){
			throw new Exception("不能连接redis! host="+host+",port="+port);
		}else{
			return jedis;
		}
	}
	
	public static void main(String[] args)throws Exception {
		Jedis jedis = RedisUtil.getJdiesInstance();
		String s = jedis.get("aaa");
		System.out.println(s);
	}
}
