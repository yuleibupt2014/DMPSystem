package com.ai.dmp.ci.tools.dbexport;

import java.io.File;

import com.ai.dmp.ci.identify.core.db.hdfs.LoadRuleFromHdfs;
import com.ai.dmp.ci.identify.mr.CIMainParam;
import org.apache.log4j.Logger;

import com.ai.dmp.ci.common.util.Config;

public class DBExportMain {
	private static Logger log = Logger.getLogger(DBExportMain.class);
	
	public static void main(String[] args) throws Exception{
		log.info(Config.help());
		
		if(args.length <= 1){
			log.info("param error ! ");
			log.info("param format: <table_NO> [<table_X_NO> <...>]");
			log.info("参数不足! 参数格式: <输出文件>  <要加密的表编号(多个编号用空格隔开)>");
			System.exit(0);
		}

        CIMainParam.initJMTools();

		for(int i=0;i<args.length;i++){
			if(!Config.tableKeys.contains(args[i])){
				log.info("param error : Error table NO!");
				log.info("参数输入错误,请仔细检查表编号");
				System.exit(0);
			}
			Config.exportTableArr.add(Config.mapTable.get(args[i]));
		}
		
		log.info("The following table will be encrypted:");
		log.info("你选择了对以下表的以下列进行加密:");
		for(String tableName:Config.exportTableArr){
			log.info(tableName+":"+Config.mapColumn.get(tableName));
		}
		
		String filename = "dmp-ci.sql";
		File file = new File(filename);
		if(file.exists()){
			file.delete();
			log.info("delete file:"+file.getPath());
		}
		
		new DBExportCore(filename).execute();

	}
}
