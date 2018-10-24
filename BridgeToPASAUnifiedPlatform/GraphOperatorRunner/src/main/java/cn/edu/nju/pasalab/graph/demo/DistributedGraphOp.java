package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.GraphOperators;

import java.util.HashMap;
import java.util.Map;

public class DistributedGraphOp {

    private String inputEdgeCSVFile;
    private String inputVertexCSVFile;
    private String graphComputerConfFile;

    public DistributedGraphOp(String inputEdgeCSVFile, String inputVertexCSVFile, String graphComputerConfFile) {
        this.inputEdgeCSVFile = inputEdgeCSVFile;
        this.inputVertexCSVFile = inputVertexCSVFile;
        this.graphComputerConfFile = graphComputerConfFile;
    }

    public void testGopCSVFileToGraph() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_EDGE_CSV_FILE_PATH, inputEdgeCSVFile);
        arguments.put(Constants.ARG_INPUT_VERTEX_CSV_FILE_PATH, inputVertexCSVFile);
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(Constants.ARG_VERTEX_NAME_COLUMN, "name");
        arguments.put(Constants.ARG_DIRECTED, "true");
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputEdgeCSVFile + ".graph");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_SPARK_GRAPHX);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, "weight");
        arguments.put(Constants.ARG_VERTEX_PROPERTY_COLUMNS, "test");
        GraphOperators op = new GraphOperators();
        op.GopCSVFileToGraph(arguments);
    }


    public void testGopLabelPropagation() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputEdgeCSVFile + ".graph");
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_SPARK_GRAPHX);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputEdgeCSVFile + ".aftergraphxlp");
        GraphOperators op = new GraphOperators();
        op.GopLabelPropagation(arguments);
    }

    public void testGopGraphToCSVFile() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputEdgeCSVFile + ".aftergraphxlp");
        arguments.put(Constants.ARG_VERTEX_PROPERTY_NAMES, "clusterID");
        arguments.put(Constants.ARG_EDGE_PROPERTY_NAMES, "weight");
        arguments.put(Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, inputVertexCSVFile + ".out");
        arguments.put(Constants.ARG_OUTPUT_EDGE_CSV_FILE_PATH, inputEdgeCSVFile + ".out");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_SPARK_GRAPHX);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        GraphOperators op = new GraphOperators();
        op.GopGraphToCSVFile(arguments);
    }

    public void run() throws Exception {
        testGopCSVFileToGraph();
        //testGopLabelPropagation();
        //testGopGraphToCSVFile();
    }

    public static void main(String args[]) throws Exception {
        String inputVertexCSVFile = "/home/lijunhong/graphxtosontest/vertex.csv";
        //String inputVertexCSVFile = null;
        String inputEdgeCSVFile = "/home/lijunhong/graphxtosontest/test.csv";
        String graphComputerPath = "/home/lijunhong/IdeaProjects/GraphOperator/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/graph-computer/SparkLocal.conf";
        DistributedGraphOp op = new DistributedGraphOp(inputEdgeCSVFile, inputVertexCSVFile,graphComputerPath);
        op.run();
    }
}
