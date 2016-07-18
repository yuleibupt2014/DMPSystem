package com.ai.dmp.ci.common.util;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsUtil {
    private static Logger log = Logger.getLogger(HdfsUtil.class);

    /**
     * 删除HDFS 文件或目录
     *
     * @param fs
     * @param pathList
     * @throws Exception
     */
    public static void deletePath(FileSystem fs, List<Path> pathList) throws Exception {
        for (Path p : pathList) {
            if (fs.exists(p)) {
                log.info("删除：" + p);
                fs.delete(p);
            }
        }
    }

    /**
     * 删除pathList下的所有空文件
     *
     * @param fs
     * @param pathList
     * @throws Exception
     */
    public static void deleteNullFile(FileSystem fs, List<Path> pathList) throws Exception {
        for (Path p : pathList) {
            deleteNullFile(fs, p);
        }
    }

    /**
     * 删除path下的所有空文件
     *
     * @param fs
     * @param path
     * @throws Exception
     */
    public static void deleteNullFile(FileSystem fs, Path path) throws Exception {
        if (!fs.exists(path)) {
            return;
        }
        FileStatus[] fileStatus = fs.listStatus(path);
        for (FileStatus file : fileStatus) {
            if (file.isFile()) {
                if (file.getLen() == 0) {//空文件
                    fs.delete(file.getPath(), true);
                }
            } else {
                deleteNullFile(fs, file.getPath());
            }
        }
    }

    public static void writeFile(FileSystem fs, String hdfsFileName, String value) {
        try {
            Path f = new Path(hdfsFileName);

            FSDataOutputStream os = fs.create(f, true);
            Writer out = new OutputStreamWriter(os, "utf-8");//以UTF-8格式写入文件，不乱码
            out.write(value);
            out.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
