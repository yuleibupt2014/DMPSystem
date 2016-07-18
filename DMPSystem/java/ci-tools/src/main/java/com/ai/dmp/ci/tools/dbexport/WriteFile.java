package com.ai.dmp.ci.tools.dbexport;

import java.io.FileOutputStream;
import java.io.Writer;

import org.apache.log4j.Logger;

public class WriteFile {
	private static Logger log = Logger.getLogger(WriteFile.class);
	private Writer out; 
	private String tablename;
	private String[] colNames;
	private String sql = null;
	
	public WriteFile(Writer out,String tablename,String[] colNams){
		this.out = out;
		this.tablename = tablename;
		this.colNames = colNams;
		init();
	}
	/**
	 * 写入 delete sql.
	 * @throws Exception
	 */
	public void writeHead()throws Exception{
		String sql="delete from "+tablename+";\n";
		out.write(sql);
		out.flush();
	}
	/**
	 * 写入insert sql
	 * @param colValues
	 * @throws Exception
	 */
	public void write(Object[] colValues)throws Exception{
		String sql = getSql(colValues);
		out.write(sql+"\n");
		out.flush();
	}
	
	public String getSql(Object[] colValues){
		StringBuilder sqlBuilder = new StringBuilder(sql);
		String value = "";
		for(int i=0;i<colValues.length;i++){
			if(colValues[i] != null && !"".equals(colValues[i])){
				value = colValues[i].toString();
			}else{
				value = "";
			}
			
			sqlBuilder.append("'").append(value).append("'");
			if(i < colValues.length-1){
				sqlBuilder.append(",");
			}
		}
		sqlBuilder.append(");");
		return sqlBuilder.toString();
	}
	
	public void init(){
		
		StringBuilder sqlBuilder = new StringBuilder("");
		sqlBuilder.append("insert into ");
		sqlBuilder.append(tablename);
		sqlBuilder.append(" (");
		for(int i=0;i<colNames.length;i++){
			sqlBuilder.append(colNames[i]);
			if(i < colNames.length-1){
				sqlBuilder.append(",");
			}
		}
		sqlBuilder.append(") ");
		sqlBuilder.append("values (");
		
		sql = sqlBuilder.toString();
	}
}
