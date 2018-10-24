package cn.edu.nju.pasalab.graph;

import cn.edu.nju.pasalab.graph.impl.external.CSVFileToGraph;
import cn.edu.nju.pasalab.graph.impl.external.labelpropagation.LabelPropagationGraphComputer;
import cn.edu.nju.pasalab.graph.impl.external.labelpropagation.LabelPropagationGraphX;
import cn.edu.nju.pasalab.graph.impl.external.GraphToCSVFile;
import cn.edu.nju.pasalab.graph.server.GraphOperatorsService;

import java.util.Map;

/**
 * The base class for graph operators.
 * This class dispatches the graph operations to specific operators.
 */
public class GraphOperators implements GraphOperatorsService.Iface{

    public void GopCSVFileToGraph(Map<String, String> arguments) throws Exception {
        String outputGraphType = (String)arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);
        if (outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
            CSVFileToGraph.toGraphSON(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + outputGraphType);
        }
    }

    public void GopGraphToCSVFile(Map<String, String> arguments) throws Exception {
        String inputGraphType = (String)arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);
        if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
            GraphToCSVFile.fromGraphSON(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + inputGraphType);
        }
    }

    public void GopLabelPropagation(Map<String, String> arguments) throws Exception {
        String inputGraphType = (String)arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_TINKERPOP_GRAPHCOMPUTER)) {
            String outputGraphType = (String)arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);
            if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                    && outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
                LabelPropagationGraphComputer.fromGraphSONToGraphSON(arguments);
            } else {
                throw new UnsupportedOperationException("No implementation for input/output graph type combination:"
                        + inputGraphType + "/" + outputGraphType);
            }

        } else if(runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            String outputGraphType = (String)arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);
            if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                    && outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
                LabelPropagationGraphX.fromGraphSONToGraphSON(arguments);
            } else {
                throw new UnsupportedOperationException("No implementation for input/output graph type combination:"
                        + inputGraphType + "/" + outputGraphType);
            }
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }

}

