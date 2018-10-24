package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.GraphOperators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoSerialGraphOperators {

    private static String gremlinServerConfFile = "demo/conf/localhost-gremlin-server.yaml";
    private static String graphName = "demoG";

    public static void main(String[] args) {
        demoCSVToGraph();
        demoGraphToCSV();
    }

    public static void demoCSVToGraph() {
        Map<String, String> arguments = new HashMap<>(), output = new HashMap<>();
        String inputCSV = "demo/demo.input.csv";
        arguments.put(Constants.ARG_CSV_FILE, inputCSV);
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(Constants.ARG_SRC_COLUMN_NAME, "src");
        arguments.put(Constants.ARG_DST_COLUMN_NAME, "dst");
        arguments.put(Constants.ARG_WEIGHT_COLUMN_NAME, "weight");
        arguments.put(Constants.ARG_DIRECTED, Boolean.TRUE.toString());
        arguments.put(Constants.ARG_OVERWRITE, Boolean.TRUE.toString());
        try {
            GraphOperators op = new GraphOperators();
            op.GopCSVFileToGraph(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("success!");
    }

    /*public static void demoPageRank() {
        Map<String, Object> arguments = new HashMap<>(), output = new HashMap<>();
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "pagerank");
        arguments.put(Constants.ARG_RETURN_TOP, new Integer(3));
        try {
            output = GraphOperators.GopCSVFileToGraph(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Elapsed time (ms): " + output.get(Constants.ARG_ELAPSED_TIME));
        System.out.println("Topest vertices: " + output.get(Constants.ARG_TOP_VERTICES));
    }*/


    public static void demoGraphToCSV() {
        Map<String, String> arguments = new HashMap<>(), output = new HashMap<>();
        String outputCSV = "demo/demo.output.csv";
        arguments.put(Constants.ARG_CSV_FILE, outputCSV);
        arguments.put(Constants.ARG_GRAPH_NAME, graphName);
        arguments.put(Constants.ARG_GREMLIN_SERVER_CONF_FILE, gremlinServerConfFile);
        List<String> properties = new ArrayList<>();
        //properties.add("pagerank"); properties.add("clusterID");
        //arguments.put(Constants.ARG_PROPERTIES, properties.toString());
        arguments.put(Constants.ARG_OVERWRITE, Boolean.TRUE.toString());
        try {
            GraphOperators op = new GraphOperators();
            op.GopGraphToCSVFile(arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("success!" );
    }




}
