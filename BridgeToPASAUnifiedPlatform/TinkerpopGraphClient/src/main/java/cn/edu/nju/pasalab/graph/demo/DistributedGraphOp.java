package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.GraphOperators;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopCSVFileToGraph;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopLabelPropagation;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopVertexPropertiesToCSVFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributedGraphOp {

    String inputCSVFile;
    String graphComputerConfFile;

    public DistributedGraphOp(String inputCSVFile, String graphComputerConfFile) {
        this.inputCSVFile = inputCSVFile;
        this.graphComputerConfFile = graphComputerConfFile;
    }

    public void testGopCSVFileToGraph() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_EDGE_CSV_FILE_PATH, inputCSVFile);
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(Constants.ARG_DIRECTED, "true");
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_HADOOP_GRAPH_COMPUTER);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, "weight");
        GraphOperators graphOperators = new GraphOperators();
        graphOperators.GopCSVFileToGraph(arguments);
    }


    public void testGopLabelPropagation() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_GRAPHX);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".aftergraphxpr");
        GraphOperators graphOperators = new GraphOperators();
        graphOperators.GopLabelPropagation(arguments);
    }

    public void testGopVertexPropertiesToCSVFile() throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".afterpr");
        arguments.put(Constants.ARG_PROPERTY_NAMES, "clusterID");
        arguments.put(Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, inputCSVFile + ".out");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_HADOOP_GRAPH_COMPUTER);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        GraphOperators graphOperators = new GraphOperators();
        graphOperators.GopVertexPropertiesToCSVFile(arguments);
    }

    public void run() throws Exception {
        //testGopCSVFileToGraph();
        testGopLabelPropagation();
        //testGopVertexPropertiesToCSVFile();
    }

    public static void main(String args[]) throws Exception {
        String inputCSVFile = "/home/lijunhong/jinyong.csv";
        String graphComputerPath = "/home/lijunhong/IdeaProjects/GraphOperator/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/graph-computer/SparkLocal.conf";
        DistributedGraphOp op = new DistributedGraphOp(inputCSVFile, graphComputerPath);
        op.run();
    }
}
