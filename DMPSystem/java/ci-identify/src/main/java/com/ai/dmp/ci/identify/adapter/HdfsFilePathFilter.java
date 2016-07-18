package com.ai.dmp.ci.identify.adapter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * Created by yulei on 2015/11/6.
 */
public class HdfsFilePathFilter implements PathFilter {
    private static Logger log = Logger.getLogger(HdfsFilePathFilter.class);

    private String fileName;

    public HdfsFilePathFilter(String fileName) {
        this.fileName = fileName;
    }

    /**
     * @param path :文件路径 如：hdfs://localhost:9000/hdfs/test/wordcount/in/word.txt
     */
    @Override
    public boolean accept(Path path) {
        log.info("path:"+path.toString());
        boolean res = false;
        if (path.toString().indexOf(fileName) != -1) {
            res = true;
        }
        return res;
    }

}
