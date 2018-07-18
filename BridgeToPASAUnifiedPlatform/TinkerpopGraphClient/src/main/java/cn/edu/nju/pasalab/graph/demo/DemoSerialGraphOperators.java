package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.GraphOperators;
import cn.edu.nju.pasalab.graph.SerialGraphOperators;
import org.apache.tinkerpop.gremlin.structure.Graph;

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
        arguments.put(GraphOperators.ARG_CSV_FILE, inputCSV);
        arguments.put(GraphOperators.ARG_GRAPH_NAME, graphName);
        arguments.put(GraphOperators.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(GraphOperators.ARG_SRC_COLUMN_NAME, "src");
        arguments.put(GraphOperators.ARG_DST_COLUMN_NAME, "dst");
        arguments.put(GraphOperators.ARG_WEIGHT_COLUMN_NAME, "weight");
        arguments.put(GraphOperators.ARG_DIRECTED, Boolean.TRUE);
        arguments.put(GraphOperators.ARG_OVERWRITE, Boolean.TRUE);
        try {
            output = SerialGraphOperators.GopCSVFileToTinkerGraphSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("|V|=" + output.get(SerialGraphOperators.ARG_NUMBER_OF_VERTICES));
        System.out.println("|E|=" + output.get(SerialGraphOperators.ARG_NUMBER_OF_EDGES));
    }

    public static void demoPageRank() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(GraphOperators.ARG_GRAPH_NAME, graphName);
        arguments.put(GraphOperators.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(GraphOperators.ARG_RESULT_PROPERTY_NAME, "pagerank");
        arguments.put(GraphOperators.ARG_RETURN_TOP, new Integer(3));
        try {
            output = SerialGraphOperators.GopPageRankWithGraphComputerSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Elapsed time (ms): " + output.get(SerialGraphOperators.ARG_ELAPSED_TIME));
        System.out.println("Topest vertices: " + output.get(SerialGraphOperators.ARG_TOP_VERTICES));
    }

    public static void demoPeerPressure() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(GraphOperators.ARG_GRAPH_NAME, graphName);
        arguments.put(GraphOperators.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(GraphOperators.ARG_RESULT_PROPERTY_NAME, "clusterID");
        try {
            output = SerialGraphOperators.GopPeerPressureWithGraphComputerSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Elapsed time (ms): " + output.get(SerialGraphOperators.ARG_ELAPSED_TIME));
        System.out.println("Number of clusters: " + output.get(SerialGraphOperators.ARG_NUMBER_OF_CLUSTERS));
    }

    public static void demoVertexTableToCSV() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(GraphOperators.ARG_CSV_FILE, outputCSV);
        arguments.put(GraphOperators.ARG_GRAPH_NAME, graphName);
        arguments.put(GraphOperators.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        List<String> properties = new ArrayList<>();
        properties.add("pagerank"); properties.add("clusterID");
        arguments.put(GraphOperators.ARG_PROPERTIES, properties);
        arguments.put(GraphOperators.ARG_OVERWRITE, Boolean.TRUE);
        try {
            output = SerialGraphOperators.GopVertexPropertiesToCSVFileSerial(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("# Lines: " + output.get(SerialGraphOperators.ARG_NUMBER_OF_LINES));
        System.out.println("File size: " + output.get(SerialGraphOperators.ARG_FILE_SIZE));
    }




}
