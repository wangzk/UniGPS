package cn.edu.nju.pasalab.graph;

import java.util.List;
import java.util.Map;

/**
 * The base class for graph operators.
 * This class dispatches the graph operations to specific operators.
 */
public class GraphOperators {

    public void GopCSVFileToGraph(Map<String, Object> arguments) throws Exception {
        String outputGraphType = (String)arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                && runMode.equals(Constants.RUNMODE_HADOOP_GRAPH_COMPUTER)) {
            cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopCSVFileToGraph.toGraphSON(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + outputGraphType);
        }
    }

    public void GopVertexPropertiesToCSVFile(Map<String, Object> arguments) throws Exception {
        String inputGraphType = (String)arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                && runMode.equals(Constants.RUNMODE_HADOOP_GRAPH_COMPUTER)) {
            cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopVertexPropertiesToCSVFile.fromGraphSON(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + inputGraphType);
        }
    }

    public void GopLabelPropagation(Map<String, Object> arguments) throws Exception {
        String inputGraphType = (String)arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_HADOOP_GRAPH_COMPUTER)) {
            String outputGraphType = (String)arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);
            if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                    && outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
                cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.GopLabelPropagation.fromGraphSONToGraphSON(arguments);
            } else {
                throw new UnsupportedOperationException("No implementation for input/output graph type combination:"
                        + inputGraphType + "/" + outputGraphType);
            }

        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }

}

