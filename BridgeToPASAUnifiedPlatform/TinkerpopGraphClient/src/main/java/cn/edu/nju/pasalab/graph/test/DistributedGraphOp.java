package cn.edu.nju.pasalab.graph.test;

import cn.edu.nju.pasalab.graph.GraphOperators;
import cn.edu.nju.pasalab.graph.impl.DistributedGraphOperatorsImpl;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
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
        arguments.put(GraphOperators.ARG_EDGE_CSV_FILE_PATH, inputCSVFile);
        arguments.put(GraphOperators.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(GraphOperators.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(GraphOperators.ARG_DIRECTED, Boolean.FALSE);
        arguments.put(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, graphComputerConfFile);
        List<String> properties = new ArrayList<>();
        properties.add("weight");
        arguments.put(GraphOperators.ARG_EDGE_PROPERTY_COLUMNS, properties);
        DistributedGraphOperatorsImpl.GopCSVFileToGryoGraphSpark(arguments);
    }


    public void testGryoPeerPressure() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".afterpr");
        arguments.put(GraphOperators.ARG_RESULT_PROPERTY_NAME, "clusterID");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, graphComputerConfFile);
        DistributedGraphOperatorsImpl.GopLabelPropagationGryoToGryo(arguments);
    }

    public void testGryoGraphVertexToCSVFile() throws Exception {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".afterpr");
        List<String> properties = new ArrayList<>();
        properties.add("clusterID");
        arguments.put(GraphOperators.ARG_PROPERTIES, properties);
        arguments.put(GraphOperators.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, inputCSVFile + ".out");
        arguments.put(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE, graphComputerConfFile);
        DistributedGraphOperatorsImpl.GopGryoGraphVertexToCSVFileSpark(arguments);
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
