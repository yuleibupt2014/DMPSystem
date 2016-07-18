package com.ai.dmp.ci.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class HiveUtil {
    private static int maxCount = 3;
    private final static Log log = LogFactory.getLog(HiveUtil.class);

    public static Process exec(String hivesql) throws Exception {
        List<String> command = new ArrayList<String>();

        command.add("hive");
        command.add("-e");
        command.add(hivesql);

        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);

        Process hiveProcess = null;
        int flag = -1;
        int count = 0;
        while(true){
            log.info(hivesql);
            hiveProcess = hiveProcessBuilder.start();
            flag = hiveProcess.waitFor();
            count ++;

            if(flag == 0){
                log.info("ok! flag=" + flag);
                break;
            }else if(count < maxCount){
                log.error("第【"+count+"】执行失败，重新执行！flag="+ flag);
            }else{
                log.error("FAILED!执行失败超过最大次数【"+maxCount+"】! flag=" + flag);
                break;
            }
        }
//		InputStream fis = hiveProcess.getInputStream();
//		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//		String line = null;
//		while ((line = br.readLine()) != null) {
//			log.info(line);
//		}
//		br.close();
//		fis.close();

        return hiveProcess;
    }


    /**
     * 执行hive语句，将结果放入List中，每个元素师hive中的每行
     * @param hivesql
     * @return
     * @throws Exception
     */
    public static List<String> exec1(String hivesql) throws Exception {
        List<String> command = new ArrayList<String>();

        command.add("hive");
        command.add("-e");
        command.add(hivesql);

        log.info(hivesql);
        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);

        Process hiveProcess = null;
        hiveProcess = hiveProcessBuilder.start();
		InputStream fis = hiveProcess.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
        List<String> lineList = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
            lineList.add(line);
		}
		br.close();
		fis.close();

        return lineList;
    }

}
