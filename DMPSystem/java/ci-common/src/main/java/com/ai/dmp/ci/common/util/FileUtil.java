package com.ai.dmp.ci.common.util;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Title: FileUtil<br>
 * Description: 与文件、IO操作相关的工具类。<br>
 * <ul>
 * <li>{@link #getBufferedInputStream(File)}：获得文件输入流，支持普通文件、gz、z、bz2压缩文件。</li>
 * <li>{@link #getBufferedOutputStream(File)}：获得文件输出流，支持普通文件、gz、bz2压缩文件。</li>
 * <li>{@link #getBufferedReader(File)}：获得文件读取器，支持普通文件、gz、z、bz2压缩文件。</li>
 * <li>{@link #getBufferedWriter(File)}：获得文件写入器，支持普通文件、gz、bz2压缩文件。</li>
 * </ul>
 *
 * @author <a href="mailto:superinkfish@gmail.com">wuqm</a> 2012-08-09 01:58
 * @version 1.0
 * @since JDK 1.5
 */
public abstract class FileUtil {
    private static Logger log = Logger.getLogger(FileUtil.class);

    /**
     * 以类路径为查找路径时，路径的起始符号
     */
    public static final String CLASSPATH_URL_PREFIX = "classpath:";

    /**
     * 通过路径获得输入流，路径不区分绝对还是相对（都会去找）
     */
    public static InputStream openInputStream(String location) throws IOException {
        InputStream is = null;
        if (location.startsWith(CLASSPATH_URL_PREFIX)) {
            is = openClasspathStream(location);
            if (is == null) {
                is = openClasspathStream(switchPath(location));
            }
        } else {
            File file = new File(location);
            if (file.exists()) {
                is = FileUtils.openInputStream(file);
            } else if ((file = new File(switchPath(location))).exists()) {
                is = FileUtils.openInputStream(file);
            }
        }
        return is;
    }

    /**
     * 通过<font color="red">已经存在的文件</font>路径获得输出流，路径不区分绝对还是相对（都会去找），而且如果在文件系统中是去覆盖已有文件的。
     */
    public static OutputStream openOutputStream(String location) throws IOException {
        OutputStream os = null;
        if (location.startsWith(CLASSPATH_URL_PREFIX)) {
            String path = location.substring(CLASSPATH_URL_PREFIX.length());
            URL url = FileUtil.class.getResource(path);
            if (url != null) {
                os = url.openConnection().getOutputStream();
            }
        } else if (!location.contains(":")) {// 利用commons-vfs的FileObject进行初始化

        } else {
            File file = new File(location);
            if (file.exists()) {
                os = FileUtils.openOutputStream(file);
            } else if ((file = new File(switchPath(location))).exists()) {
                os = FileUtils.openOutputStream(file);
            }
        }
        return os;
    }

    /**
     * 从数组中找到一个已经存在的文件，并打开输出流。数组中找到一个就退出
     *
     * @param locations
     * @return
     * @throws IOException
     */
    public static OutputStream openOutputStream(String[] locations) throws IOException {
        OutputStream os = null;
        for (int i = 0; i < locations.length; i++) {
            os = openOutputStream(locations[i]);
            if (os != null) {
                log.debug("Get output stream using: " + locations[i]);
                break;
            }
        }
        return os;
    }

    /**
     * 从数组中找到一个文件输入流.数组中找到一个就退出
     *
     * @param locations
     * @return
     * @throws IOException
     */
    public static InputStream openInputStream(String[] locations) throws IOException {
        InputStream is = null;
        for (int i = 0; i < locations.length; i++) {
            is = openInputStream(locations[i]);
            if (is != null) {
                log.debug("Get input stream: " + locations[i]);
                break;
            }
        }
        return is;
    }

    /**
     * 递归的列出一个文件夹下所有的文件，包括子文件夹下的文件
     *
     * @param file
     * @return
     */
    public static Collection<File> recursiveListFiles(File file) {
        Collection<File> collection = new ArrayList<File>();
        recursiveListFiles(file, collection);
        return collection;
    }

    /**
     * 绝对路径、相对路径的相互转换。这里的转换并非严格的转换，而是把路径前的“/”进行简单的添加或删除。
     */
    public static String switchPath(String path) {
        if (path.startsWith("/")) {
            return path.substring(1);
        } else if (path.startsWith(CLASSPATH_URL_PREFIX)) {
            return switchPath(path, CLASSPATH_URL_PREFIX);
        }
        return "/" + path;
    }

    /**
     * 从类路径得到输入流
     */
    private static InputStream openClasspathStream(String location) throws IOException {
        InputStream is = null;
        String path = location.substring(CLASSPATH_URL_PREFIX.length());
        URL url = FileUtil.class.getResource(path);
        if (url != null) {
            log.info("load from : " + url.toString());
            is = url.openStream();
        }
        return is;
    }

    /**
     * 递归函数，辅助{@link FileUtil#recursiveListFiles(File)}方法
     *
     * @param file
     * @param collection
     */
    private static void recursiveListFiles(File file, Collection<File> collection) {
        if (file.isFile()) {
            collection.add(file);
        } else {
            File[] files = file.listFiles();
            for (File f : files) {
                recursiveListFiles(f, collection);
            }
        }
    }

    private static String switchPath(String path, String prefix) {
        String _path = path.substring(prefix.length());
        if (_path.startsWith("/")) {
            return prefix + _path.substring(0);
        } else {
            return prefix + "/" + _path;
        }
    }

    public static List<String> readFromFile(InputStream in) throws Exception {
        List<String> lineList = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                lineList.add(line.trim());
            }
            reader.close();
        } catch(Exception e){
            log.error("读取文件错误！"+e.getMessage(),e);
        }

        return lineList;
    }
}