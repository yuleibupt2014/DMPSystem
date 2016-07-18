package com.ai.dmp.ci.identify.core.db.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.common.util.CIConst;

/**
 * 数据库连接操作类：能够获取和关闭连接
 * @author yulei   2014-05-16
 */
public class JdbcUtil {
	private static Logger log = Logger.getLogger(JdbcUtil.class);
	
	private static String driverclass;//数据库连接driver
	private static String url;//数据库连接url
	private static String username;//数据库用户名
	private static String password;//数据库密码
	
	private static Queue<Connection> connQueuePool = null;

	static {
        if (Config.isLoadRuleFromMysqlToMysql() || !Config.isRuleDbHdfs()){
            init();//初始化，包括读取配置文件等
        }
	}

	/**
	 * 获取连接，每次都新建连接（目前没有使用连接池）
	 * 
	 * @return
	 * @throws Exception 
	 */
	public static Connection getConnection() throws Exception {
		Connection conn = null;
		try{
            //conn = (Connection) DriverManager.getConnection(url,username,password);
			conn = connQueuePool.poll();
		}catch(Exception e){
			log.error("获取连接错误！"+e.getMessage(),e);
			throw e;
		}
		return conn;
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 */
	public static void closeConnection(Connection conn) {
		try {
			if (conn != null) {
				//conn.close();
				connQueuePool.add(conn);
			}
		} catch (Exception e) {
			log.error("关闭连接错误！" + e.getMessage(), e);
		}
	}
	
	/**
	 * 销毁连接池
	 */
	public static void destroyConnPool(){
		try{
			Connection conn = getConnection();
			if(conn != null){
				conn.close();
			}
			connQueuePool = null;
		}catch(Exception e){
			log.error("关闭连接池错误！\n"+e.getMessage(),e);
		}
	}
	
	/**
	 * 创建连接池，默认只创建一个连接
	 * @throws Exception
	 */
	public static synchronized void createConnPool() throws Exception{
		//目前连接池只放一个连接
		connQueuePool = new ConcurrentLinkedQueue<Connection>();
		Connection conn = (Connection) DriverManager.getConnection(url,username,password);
		connQueuePool.add(conn);
	}
	
	/**
	 * 初始化：读取配置文件以及创建连接池
	 */
	private static void init(){
            
		try {
			driverclass = Config.getString(CIConst.Config.RULE_DB_DRIVERCLASS);
			url = Config.getString(CIConst.Config.RULE_DB_URL);
			username = Config.getString(CIConst.Config.RULE_DB_USERNAME);
			password = Config.getString(CIConst.Config.RULE_DB_PASSWORD);
			Class.forName(driverclass);
			
			createConnPool();//创建连接池
			log.info("driverclass:"+driverclass);
			log.info("url:"+url);
			log.info("username:"+username);
			log.info("password:"+password);
			log.info("创建mysql连接成功！");
		} catch (Exception e) {
			log.error("连接数据库错误！"+e.getMessage(),e);
		}
	}
}
