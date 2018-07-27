package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.SerialGraphOperators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoSerialGraphOperators {

    static String gremlinServerConfFile = "demo/conf/localhost-gremlin-server.yaml";
    static String inputCSV = "demo/demo.input.csv";
    static String outputCSV = "demo/demo.output.csv";
    static String graphName = "demoG";

    public static void main(String[] args) {
        demoCSVToGraph();
        demoPageRank();
        demoPeerPressure();
        demoVertexTableToCSV();
    }

    public static void demoCSVToGraph() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(Constants.ARG_CSV_FILE, inputCSV);
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(Constants.ARG_SRC_COLUMN_NAME, "src");
        arguments.put(Constants.ARG_DST_COLUMN_NAME, "dst");
        arguments.put(Constants.ARG_WEIGHT_COLUMN_NAME, "weight");
        arguments.put(Constants.ARG_DIRECTED, Boolean.TRUE);
        arguments.put(Constants.ARG_OVERWRITE, Boolean.TRUE);
        try {
            output = SerialGraphOperators.GopCSVFileToTinkerGraphSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("|V|=" + output.get(Constants.ARG_NUMBER_OF_VERTICES));
        System.out.println("|E|=" + output.get(Constants.ARG_NUMBER_OF_EDGES));
    }

    public static void demoPageRank() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "pagerank");
        arguments.put(Constants.ARG_RETURN_TOP, new Integer(3));
        try {
            output = SerialGraphOperators.GopPageRankWithGraphComputerSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Elapsed time (ms): " + output.get(Constants.ARG_ELAPSED_TIME));
        System.out.println("Topest vertices: " + output.get(Constants.ARG_TOP_VERTICES));
    }

    public static void demoPeerPressure() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID");
        try {
            output = SerialGraphOperators.GopPeerPressureWithGraphComputerSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Elapsed time (ms): " + output.get(Constants.ARG_ELAPSED_TIME));
        System.out.println("Number of clusters: " + output.get(Constants.ARG_NUMBER_OF_CLUSTERS));
    }

    public static void demoVertexTableToCSV() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(Constants.ARG_CSV_FILE, outputCSV);
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        List<String> properties = new ArrayList<>();
        properties.add("pagerank"); properties.add("clusterID");
        arguments.put(Constants.ARG_PROPERTIES, properties);
        arguments.put(Constants.ARG_OVERWRITE, Boolean.TRUE);
        try {
            output = SerialGraphOperators.GopVertexPropertiesToCSVFileSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("# Lines: " + output.get(Constants.ARG_NUMBER_OF_LINES));
        System.out.println("File size: " + output.get(Constants.ARG_FILE_SIZE));
    }




}
