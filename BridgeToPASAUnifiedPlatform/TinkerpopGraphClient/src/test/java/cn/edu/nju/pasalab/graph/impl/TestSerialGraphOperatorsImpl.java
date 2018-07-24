package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.GraphOperators;
import cn.edu.nju.pasalab.graph.SerialGraphOperators;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestSerialGraphOperatorsImpl {
    String testGraphName = "test";

    private static void printFileContent(String filePath) throws IOException {
        FileSystem fs = HDFSUtils.getDefaultFS();
        FSDataInputStream stream = fs.open(new Path(filePath));
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
        stream.close();
    }

    @Test
    public void comprehensiveTest() throws Exception {
        testGopCSVFileToTinkerGraphSerial();
        testPageRank();
        testPeerPressure();
        testGopVertexTableToCSVFileSerial();
    }

    @Test
    public void testGopCSVFileToTinkerGraphSerial() throws Exception {
        String csvFile = "/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/src/test/java/cn/edu/nju/pasalab/graph/impl/test.input.csv";
        String remoteConfFile = "/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/test-remote.yaml";
        Map<String, Object> output =
        SerialGraphOperatorsImpl.GopCSVFileToTinkerGraphSerial(csvFile, testGraphName, remoteConfFile, "src", "dst", "weight", true, true);
        System.out.println("Num V:" + output.get(GraphOperators.ARG_NUMBER_OF_VERTICES));
        System.out.println("Num E:" + output.get(GraphOperators.ARG_NUMBER_OF_EDGES));
    }

    @Test
    public void testGopVertexTableToCSVFileSerial() throws Exception {
        String csvFile = "test.output.csv";
        String remoteConfFile = "/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/test-remote.yaml";
        List<String> properties = new ArrayList<>();
        properties.add("name");properties.add("pr2");properties.add("cluster");
        Map<String, Object> output =
                SerialGraphOperatorsImpl.GopVertexPropertiesToCSVFileSerial(testGraphName, remoteConfFile, csvFile, properties,true);
        printFileContent(csvFile);
        HDFSUtils.getDefaultFS().delete(new Path(csvFile), true);
        System.out.println(output.get(SerialGraphOperators.ARG_NUMBER_OF_LINES));
        System.out.println(output.get(SerialGraphOperators.ARG_FILE_SIZE));
    }

    @Test
    public void testPageRank() throws Exception {
        String remoteConfFile = "/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/test-remote.yaml";
        Map<String, Object> output = SerialGraphOperatorsImpl.GopPageRankWithGraphComputerSerial(testGraphName, remoteConfFile, "pr2", 0);
        System.out.println(output);
    }

    @Test
    public void testPeerPressure() throws Exception {
        String remoteConfFile = "/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/test-remote.yaml";
        Map<String, Object> output = SerialGraphOperatorsImpl.GopPeerPressureWithGraphComputerSerial(testGraphName, remoteConfFile, "cluster");
        System.out.println(output);
    }

    @Test
    public void testGopCSVFileToGryoGraphSerial() throws Exception {
        String demoDir = "dist/demo/jinyong";
        String outputGryoFile = "jinyong.kryo";
        ArrayList<String> weight = new ArrayList<>();
        weight.add("weight");
        SerialGraphOperatorsImpl.GopCSVFileToGryoGraphSingleClient(demoDir, "p1", "p2",
                weight, false, outputGryoFile);
    }
    @Test
    public void testGopGryoGraphVertexToCSVFileSingleClient() throws Exception {
        String inputGryoFile = "jinyong.cluster.kryo";
        String outputDir = "jinyong.csv";
        ArrayList<String> properties = new ArrayList<>();
        properties.add("cluster");
        SerialGraphOperatorsImpl.GopGryoGraphVertexToCSVFileSingleClient(inputGryoFile, properties, outputDir);
    }

    @Test
    public void testGopLabelPropagationGryoToGryoSingleClient() throws Exception {
        String inputGryoFile = "jinyong.kryo";
        String outputGryoFile = "jinyong.cluster.kryo";
        SerialGraphOperatorsImpl.GopLabelPropagationGryoToGryoSingleClient(inputGryoFile, outputGryoFile, "cluster");
    }

}
