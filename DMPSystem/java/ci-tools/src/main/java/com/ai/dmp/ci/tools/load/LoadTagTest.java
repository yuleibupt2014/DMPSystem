package com.ai.dmp.ci.tools.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

public class LoadTagTest {

	public static void main(String[] args)throws Exception{
		FileOutputStream out = new FileOutputStream("C:/Users/yulei.AILK/Desktop/aa.sql");
		String fileName = "C:/Users/yulei.AILK/Desktop/cookie_id.txt";
		String sql = "INSERT INTO dim_ci_match_token VALUES (";
		
		File file = new File(fileName);
		BufferedReader reader = null;
		reader = new BufferedReader(new FileReader(file));
		String tempString = null;
		
		StringBuilder sqlBuilder = null;
		int i = 1;
		while ((tempString = reader.readLine()) != null) {
			if(tempString!=null && !"".equals(tempString)){
				sqlBuilder = new StringBuilder(sql);
				sqlBuilder.append("'").append(i).append("',");
				String[] arr = tempString.split("\t");
				sqlBuilder.append("'").append(arr[0]).append("',");
				sqlBuilder.append("null,null,");
				sqlBuilder.append("'").append(arr[1]);
				if(arr[1].toLowerCase().equals("baiduid")){
					sqlBuilder.append("=([^;]{1,32})',");
					sqlBuilder.append("'baidu_',");
				}else if(arr[1].toLowerCase().equals("__utma")){
					sqlBuilder.append("=([^;]{3,}\\\\.[^;]{1,})\\\\.[^;]{1,}\\\\.[^;]{1,}\\\\.[^;]{1,}\\\\.[^;]{1,}',");
					sqlBuilder.append("'"+arr[0]+"_',");
				}else if(arr[1].toLowerCase().equals("tma")){
					sqlBuilder.append("=([^;]{3,}\\\\.[^;]{1,})\\\\.[^;]{1,}\\\\.[^;]{1,}\\\\.[^;]{1,}\\\\.[^;]{1,}',");
					sqlBuilder.append("'"+arr[0]+"_',");
				}else if (arr[1].toLowerCase().equals("cna")){
					sqlBuilder.append("=([^;]{1,})',");
					sqlBuilder.append("'ali_',");
				}else{
					sqlBuilder.append("=([^;]{1,})',");
					sqlBuilder.append("'"+arr[0]+"_',");
				}
				sqlBuilder.append("'cookie_id');");
				sqlBuilder.append("\n");
				out.write(sqlBuilder.toString().getBytes());
				i++;
			}
		}
	}
}
