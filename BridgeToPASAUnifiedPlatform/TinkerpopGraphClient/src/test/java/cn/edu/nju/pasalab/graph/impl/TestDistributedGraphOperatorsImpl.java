package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopCSVFileToGraph;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopLabelPropagation;
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopVertexPropertiesToCSVFile;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDistributedGraphOperatorsImpl {
    @Test
    public void testCSVToGryoFileSpark() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_EDGE_CSV_FILE_PATH, "dist/demo/jinyong");
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(Constants.ARG_DIRECTED, Boolean.FALSE);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.gryo");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        List<String> properties = new ArrayList<>();
        properties.add("weight");
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, properties);

        GopCSVFileToGraph.toGraphSON(arguments);
    }

    @Test
    public void testGryoGraphVertexToCSVFile() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.pr");
        List<String> properties = new ArrayList<>();
        properties.add("clusterID");
        arguments.put(Constants.ARG_PROPERTIES, properties);
        arguments.put(Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, "dist/demo/jinyong.out");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        GopVertexPropertiesToCSVFile.fromGraphSON(arguments);
    }

    @Test
    public void testGryoPeerPressure() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.gryo");
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, "dist/demo/jinyong.pr");
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, "conf/graph-computer/SparkLocal.conf");
        GopLabelPropagation.fromGraphSONToGraphSON(arguments);
    }
}
