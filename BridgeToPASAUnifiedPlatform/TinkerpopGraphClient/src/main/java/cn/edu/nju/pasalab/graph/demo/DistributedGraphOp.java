package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
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

    public void testCSVToGryoFileSpark() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_EDGE_CSV_FILE_PATH, inputCSVFile);
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(Constants.ARG_DIRECTED, Boolean.FALSE);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        List<String> properties = new ArrayList<>();
        properties.add("weight");
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, properties);
        GopCSVFileToGraph.toGraphSON(arguments);
    }


    public void testGryoPeerPressure() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".afterpr");
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        GopLabelPropagation.fromGraphSONToGraphSON(arguments);
    }

    public void testGryoGraphVertexToCSVFile() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".afterpr");
        List<String> properties = new ArrayList<>();
        properties.add("clusterID");
        arguments.put(Constants.ARG_PROPERTIES, properties);
        arguments.put(Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, inputCSVFile + ".out");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        GopVertexPropertiesToCSVFile.fromGraphSON(arguments);
    }

    public void run() throws Exception {
        testCSVToGryoFileSpark();
        testGryoPeerPressure();
        testGryoGraphVertexToCSVFile();
    }

    public static void main(String args[]) throws Exception {
        String inputCSVFile = args[0];
        String graphComputerPath = args[1];
        DistributedGraphOp op = new DistributedGraphOp(inputCSVFile, graphComputerPath);
        op.run();
    }
}
