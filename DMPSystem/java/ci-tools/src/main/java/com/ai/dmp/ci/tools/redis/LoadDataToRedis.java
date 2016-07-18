package com.ai.dmp.ci.tools.redis;

import java.io.BufferedReader;
import java.io.FileReader;

import com.ai.dmp.ci.identify.conf.Config;
import com.ai.dmp.ci.identify.core.db.util.RedisUtil;
import com.ai.dmp.ci.common.util.CIConst;

public class LoadDataToRedis {
	private static String instanceKeyPre = "asia-dmp-ci:";//实例库key前缀

	public static void main(String[] args) {
		String fileName = "";
		String tagId = "";
		
		if(args == null || args.length !=2){
			System.out.println("error args!");
			return;
		}else{
			fileName = args[0];
			tagId = args[1];
		}
		insertToRedis(fileName,tagId);
	}
	
	public static void insertToRedis(String file,String tagId){
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 0;
			System.out.println(file);
			while ((tempString = reader.readLine()) != null) {
				if(tempString!=null && !"".equals(tempString)){
					String key = instanceKeyPre+tempString;
					if(line<=10)
					System.out.println(key+"           "+tagId);
					RedisUtil.getJdiesInstance().set(key, tagId);
					line++;
					if(line%1000==0){
//						RedisUtil.getJdiesInstance().flushAll();
						System.out.println("finished: "+line);
					}
				}
			}
			System.out.println("finished, total: "+line);
//			RedisUtil.getJdiesInstance().flushAll();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
}
