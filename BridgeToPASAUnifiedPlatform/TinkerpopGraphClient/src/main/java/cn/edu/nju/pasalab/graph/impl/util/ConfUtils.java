package cn.edu.nju.pasalab.graph.impl.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConfUtils {

    public static PropertiesConfiguration loadConfFromHDFS(String hdfsFilePath) throws IOException, ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        InputStream fileStream = HDFSUtils.openFile(hdfsFilePath);
        conf.load(fileStream);
        fileStream.close();
        return conf;
    }

    public static void saveConfToHDFS(PropertiesConfiguration conf,String hdfsFilePath) throws IOException, ConfigurationException {
        OutputStream fileStream = HDFSUtils.createFile(hdfsFilePath);
        conf.save(fileStream);
        fileStream.close();
    }

    public static PropertiesConfiguration loadExtraConfFromHDFS(PropertiesConfiguration conf, String hdfsFilePath) throws IOException, ConfigurationException {
        InputStream fileStream = HDFSUtils.openFile(hdfsFilePath);
        conf.load(fileStream);
        fileStream.close();
        return conf;
    }

    public static void loadUserConfToSparkConf(SparkConf sparkConf, Configuration userConf) {
        userConf.getKeys().forEachRemaining(key -> {
            String value = userConf.getString(key);
            sparkConf.set(key, value);
        });
    }

}
