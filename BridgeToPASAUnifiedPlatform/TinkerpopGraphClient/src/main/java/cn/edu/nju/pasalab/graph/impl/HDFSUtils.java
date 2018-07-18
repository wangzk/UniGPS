package cn.edu.nju.pasalab.graph.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.mortbay.util.IO;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HDFSUtils {

    public static Configuration getHadoopConf() {
        return new Configuration();
    }

    public static FileSystem getFS(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    public static FileSystem getDefaultFS() throws IOException {
        return getFS(getHadoopConf());
    }

    public static FileSystem getLocalFS() throws IOException {
        return FileSystem.getLocal(getHadoopConf());
    }

    public static FileSystem getFS(String path) throws IOException {
        Configuration hadoopConf = new Configuration();
        URI uri = URI.create(path);
        return FileSystem.get(uri, hadoopConf);
    }

    public static File getHDFSFileToTmpLocal(String hdfsFilePath) throws IOException {
        String localFileName =  "tmplocal-" + new Path(hdfsFilePath).getName() + System.currentTimeMillis();
        File localFile = new File(localFileName);
        boolean isOK = FileUtil.copy(getDefaultFS(), new Path(hdfsFilePath), localFile, false, getHadoopConf());
        if (!isOK) {
            throw new IOException("Copy the hdfs file " + hdfsFilePath + " to local " + localFile + " fails!");
        }
        return localFile;
    }

    public static boolean deleteLocalTmpFile(File tmpFile) throws IOException {
        return tmpFile.delete();
    }
}
