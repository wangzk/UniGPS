package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.Edge;
import cn.edu.nju.pasalab.graph.GraphOperators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.*;
import java.util.*;

public class SerialGraphOperatorsImpl {


    /**
     * Name: GopCSVFileToTinkerGraphSerial
     *
     * Action: 将一个保存在HDFS上的CSV文件，转换为一个保存在TinkerPop兼容的图数据库中的图。单机串行实现。
     *
     * Input:
     *
     *     csvFile(String). The file path to the csv file on HDFS. The CSV file should have a header and at least two columns src and dst. Each line of the CSV file represents a directed simple edge from src to dst. If there is a weight column, it will be interpreted as the weight property of the edge. The type of weight is double in the database. The src and dst will be treated as the values of the name property of the vertices in the graph.
     *     graphName(String). The name of the input graph. GraphName will be treated as the label attached on each vertex and edge in this CSV file.
     *     gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
     *     deleteExistingGraph (Boolean, optional, default = true). Whether or not delete the existing graph with the label GraphName in the database before loading the new graph in the CSV file. If DeleteExistingGraph = false and the existing graph conflicts with the new graph, exceptions will be thrown.
     *
     * Output:
     *
     *     numberOfVertices(Long). Number of vertices in the graph.
     *     numberOfEdges(Long). Number of edges in the graph.
     */
    public static Map<String, Object> GopCSVFileToTinkerGraphSerial(String csvFile,
                                                      String graphName,
                                                      String gremlinServerConfFile,
                                                      boolean deleteExistingGraph) throws Exception {
        Logger logger = Logger.getLogger(SerialGraphOperatorsImpl.class);
        File localGremlinServerConfFile = HDFSUtils.getHDFSFileToTmpLocal(gremlinServerConfFile);
        SerialGraphLoader loader = new SerialGraphLoader(localGremlinServerConfFile, graphName);
        HDFSUtils.deleteLocalTmpFile(localGremlinServerConfFile);
        ///////////
        if (deleteExistingGraph) {
            logger.info("Start deleting the existing graph " + graphName);
            loader.clearVertices();
        }
        ///////////// Store to the graph
        FileSystem fs = HDFSUtils.getFS(csvFile);
        FSDataInputStream csvFileStream = fs.open(new Path(csvFile));
        Reader in = new InputStreamReader(csvFileStream);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        logger.info("Get CSV file header:" + ((CSVParser) records).getHeaderMap());
        ArrayList<Edge> tmpEdgeLists = new ArrayList<>();
        for(CSVRecord record: records) {
            String src = record.get("src");
            String dst = record.get("dst");
            if (record.isSet("weight")) {
                Double weight = new Double(record.get("weight"));
                tmpEdgeLists.add(new Edge(src, dst, weight));
            } else {
                tmpEdgeLists.add(new Edge(src, dst));
            }
            if (tmpEdgeLists.size() >= 100) {
                loader.addE(tmpEdgeLists, true);
            }
        }
        if (tmpEdgeLists.size() > 0) {
            loader.addE(tmpEdgeLists, true);
        }
        logger.info("Load graph done!");
        long numV = loader.getNumOfV();
        long numE = loader.getNumofE();
        HDFSUtils.deleteLocalTmpFile(localGremlinServerConfFile);
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_NUMBER_OF_VERTICES, new Long(numV));
        output.put(GraphOperators.ARG_NUMBER_OF_EDGES, new Long(numE));
        loader.close();
        return output;
    }

    public static Map<String, Object> GopVertexTableToCSVFileSerial(String graphName,
                                                                    String gremlinServerConfFile,
                                                                    String csvFile,
                                                                    List<String> properties,
                                                                    boolean deleteExistingFile) throws Exception {
        Logger logger = Logger.getLogger(SerialGraphOperatorsImpl.class);
        SerialGraphLoader loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        //////////////
        // Prepare for output
        if (!properties.contains("name")) {
            properties.add(0, "name");
        }
        FileSystem fs = HDFSUtils.getFS(csvFile);
        FSDataOutputStream csvFileStream = fs.create(new Path(csvFile), deleteExistingFile);
        PrintStream csvStream = new PrintStream(csvFileStream);
        CSVPrinter printer = CSVFormat.DEFAULT.withHeader(properties.toArray(new String[properties.size()])).print(csvStream);
        logger.info("Start writing vertex properties...");
        //////////////
        // Vid list
        Iterator<Map<String, Object>> vProperties = loader.getVertexProperties().iterator();
        List<Object> record = new ArrayList<>();
        long numberOfLines = 0L;
        while (vProperties.hasNext()) {
            Map<String, Object> vertexProperties = vProperties.next();
            for(String p: properties) {
                record.add(((ArrayList)vertexProperties.get(p)).get(0));
            }
            printer.printRecord(record);
            numberOfLines ++;
            record.clear();
        }
        printer.close();csvStream.close();csvFileStream.close();
        logger.info("Done!");
        long fileSize = fs.getFileStatus(new Path(csvFile)).getLen();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_NUMBER_OF_LINES, numberOfLines);
        output.put(GraphOperators.ARG_FILE_SIZE, fileSize);
        loader.close();
        return output;
    }

    public static Map<String, Object> GopPageRankWithGraphComputerSerial(String graphName,
                                                                         String gremlinServerConfFile,
                                                                         String resultPropertyName,
                                                                         int returnTop) throws Exception {
        SerialGraphLoader loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        long t0 = System.currentTimeMillis();
        Map<String, Double> topRanks = loader.pageRank(resultPropertyName, returnTop);
        long t1 = System.currentTimeMillis();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_ELAPSED_TIME, (t1 - t0));
        if (returnTop > 0)
            output.put(GraphOperators.ARG_TOP_VERTICES, topRanks);
        loader.close();
        return output;
    }

    public static Map<String, Object> GopPeerPressureWithGraphComputerSerial(String graphName,
                                                                         String gremlinServerConfFile,
                                                                         String resultPropertyName) throws Exception {
        SerialGraphLoader loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        long t0 = System.currentTimeMillis();
        long clusterCount = loader.peerPressure(resultPropertyName);
        long t1 = System.currentTimeMillis();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_ELAPSED_TIME, (t1 - t0));
        output.put(GraphOperators.ARG_NUMBER_OF_CLUSTERS, clusterCount);
        loader.close();
        return output;
    }

    private static SerialGraphLoader getLoaderFromConfFile(String gremlinConfFileOnHDFS, String graphName) throws IOException {
        File localGremlinServerConfFile = HDFSUtils.getHDFSFileToTmpLocal(gremlinConfFileOnHDFS);
        SerialGraphLoader loader = new SerialGraphLoader(localGremlinServerConfFile, graphName);
        HDFSUtils.deleteLocalTmpFile(localGremlinServerConfFile);
        return loader;
    }
}
