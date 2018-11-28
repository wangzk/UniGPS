package cn.edu.nju.pasalab.graph;

import cn.edu.nju.pasalab.graph.impl.external.CSVFileToGraph;
import cn.edu.nju.pasalab.graph.impl.external.labelpropagation.LabelPropagationGraphComputer;
import cn.edu.nju.pasalab.graph.impl.external.labelpropagation.LabelPropagationGraphX;
import cn.edu.nju.pasalab.graph.impl.external.GraphToCSVFile;
import cn.edu.nju.pasalab.graph.server.GraphOpService;

import java.util.HashMap;
import java.util.Map;

/**
 * The base class for graph operators.
 * This class dispatches the graph operations to specific operators.
 */
public class GraphOperators implements GraphOpService.Iface {

    public String GopCSVFileToGraph(String inputEdgeCSVFilePath, String edgeSrcColumn,
                                  String edgeDstColumn, String edgePropertyColumns,
                                  String directed, String outputGraphType,
                                  String outputGraphConfFile, String runMode,
                                  String runModeConfFile, String inputVertexCSVFilePath,
                                  String vertexNameColumn, String vertexPropertyColumns) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_EDGE_CSV_FILE_PATH, inputEdgeCSVFilePath);
        arguments.put(Constants.ARG_INPUT_VERTEX_CSV_FILE_PATH, inputVertexCSVFilePath);
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, edgeSrcColumn);
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, edgeDstColumn);
        arguments.put(Constants.ARG_VERTEX_NAME_COLUMN, vertexNameColumn);
        arguments.put(Constants.ARG_DIRECTED, directed);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, outputGraphConfFile);
        arguments.put(Constants.ARG_RUNMODE, runMode);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, outputGraphType);
        if (!runModeConfFile.equals("")){
            arguments.put(Constants.ARG_RUNMODE_CONF_FILE, runModeConfFile);
        }
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, edgePropertyColumns);
        arguments.put(Constants.ARG_VERTEX_PROPERTY_COLUMNS, vertexPropertyColumns);
        if (outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
            CSVFileToGraph.toGraphSON(arguments);
        } else if (outputGraphType.equals(Constants.GRAPHTYPE_GRAPHDB_NEO4J)) {
            CSVFileToGraph.toGraphDB(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + outputGraphType);
        }

        return "success";
    }



    public String GopGraphToCSVFile(String inputGraphType, String inputGraphConfFile,
                                  String vertexPropertyNames, String outputVertexCSVFile,
                                  String edgePropertyNames, String outputEdgeCSVFile,
                                  String runMode, String runModeConfFile) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputGraphConfFile);
        arguments.put(Constants.ARG_VERTEX_PROPERTY_NAMES, vertexPropertyNames);
        arguments.put(Constants.ARG_EDGE_PROPERTY_NAMES, edgePropertyNames);
        arguments.put(Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH, outputVertexCSVFile);
        arguments.put(Constants.ARG_OUTPUT_EDGE_CSV_FILE_PATH, outputEdgeCSVFile);
        arguments.put(Constants.ARG_RUNMODE, runMode);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, runModeConfFile);
        if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
            GraphToCSVFile.fromGraphSON(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + inputGraphType);
        }
        return "success";
    }

    public String GopLabelPropagation(String inputGraphType, String inputGraphConfFile,
                                    String resultPropertyName, String runMode,
                                    String runModeConfFile, String outputGraphType,
                                    String outputGraphConfFile) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputGraphConfFile);
        arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, resultPropertyName);
        arguments.put(Constants.ARG_RUNMODE, runMode);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, runModeConfFile);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, outputGraphType);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, outputGraphConfFile);

        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_TINKERPOP_GRAPHCOMPUTER)) {
            if (inputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)
                    && outputGraphType.equals(Constants.GRAPHTYPE_GRAPHSON)) {
                LabelPropagationGraphComputer.fromGraphSONToGraphSON(arguments);
            } else {
                throw new UnsupportedOperationException("No implementation for input/output graph type combination:"
                        + inputGraphType + "/" + outputGraphType);
            }

        } else if(runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
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

        return "success";
    }

}

