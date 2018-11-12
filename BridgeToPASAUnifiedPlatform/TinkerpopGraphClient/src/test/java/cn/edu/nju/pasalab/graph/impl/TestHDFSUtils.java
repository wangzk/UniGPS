package cn.edu.nju.pasalab.graph.impl;

import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestHDFSUtils {
    @Test
    public void testGetHadoopConf() {
        Configuration conf = HDFSUtils.getHadoopConf();
        assertNotNull(conf);
        System.out.println(conf);
    }

    @Test
    public void testGetFS() throws IOException {
        FileSystem defaultFS = HDFSUtils.getDefaultFS();
        assertNotNull(defaultFS);
        System.out.println(defaultFS);
        FileSystem localFS = HDFSUtils.getLocalFS();
        assertNotNull(localFS);
        System.out.println(localFS);
    }

    @Test
    public void testTempFileGet() throws IOException {
        File localFile = new File("testFile");
        localFile.createNewFile();
        assertTrue(localFile.exists());
        File copiedFile = HDFSUtils.getHDFSFileToTmpLocal(localFile.getPath());
        assertNotNull(copiedFile);
        assertTrue(copiedFile.exists());
        System.out.println("Copied file: " + copiedFile);
        HDFSUtils.deleteLocalTmpFile(copiedFile);
        assertFalse(copiedFile.exists());
        localFile.delete();
    }

}
