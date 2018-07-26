package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.GraphOperators;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDistributedGraphOperatorsImpl {
    @Test
    public void testCSVToGryoFileSpark() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GraphOperators.ARG_EDGE_CSV_FILE_PATH, "dist/demo/jinyong");
        arguments.put(GraphOperators.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(GraphOperators.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(GraphOperators.ARG_DIRECTED, Boolean.FALSE);
        arguments.put(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.gryo");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        List<String> properties = new ArrayList<>();
        properties.add("weight");
        arguments.put(GraphOperators.ARG_EDGE_PROPERTY_COLUMNS, properties);

        DistributedGraphOperatorsImpl.GopCSVFileToGryoGraphSpark(arguments);
    }

    @Test
    public void testGryoGraphVertexToCSVFile() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.pr");
        List<String> properties = new ArrayList<>();
        properties.add("clusterID");
        arguments.put(GraphOperators.ARG_PROPERTIES, properties);
        arguments.put(GraphOperators.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, "dist/demo/jinyong.out");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        DistributedGraphOperatorsImpl.GopGryoGraphVertexToCSVFileSpark(arguments);
    }

    @Test
    public void testGryoPeerPressure() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.gryo");
        arguments.put(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.pr");
        arguments.put(GraphOperators.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        DistributedGraphOperatorsImpl.GopLabelPropagationGryoToGryo(arguments);
    }
}
