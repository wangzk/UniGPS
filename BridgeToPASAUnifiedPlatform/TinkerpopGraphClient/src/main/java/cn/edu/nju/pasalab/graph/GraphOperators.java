package cn.edu.nju.pasalab.graph;

import cn.edu.nju.pasalab.graph.impl.SerialGraphOperatorsImpl;

import java.util.List;
import java.util.Map;

/**
 * The base class for graph operators.
 */
public class GraphOperators {


    public static final String TYPE_TINKERPOPDB_DIRECT = "TinkerPopDBDirect";
    public static final String TYPE_TINKERPOPDB_GS = "TinkerPopDBGremlinServer";
    public static final String TYPE_GRYOFILE = "GryoFile";

    public static final String MODE_SINGLE_CLIENT = "SingleClient";
    public static final String MODE_GS_GRAPHCOMPUTER = "GSGraphComputer";
    public static final String MODE_GIRAPH_GRAPHCOMPUTER = "GiraphGraphCpmputer";

    public static final String ARG_CSV_FILE = "csvFile";
    public static final String ARG_GRAPH_NAME = "graphName";
    public static final String ARG_GREMLIN_SERVER_CONF_FILE = "gremlinServerConfFile";
    public static final String ARG_PROPERTIES = "properties";
    public static final String ARG_RESULT_PROPERTY_NAME = "resultPropertyName";
    public static final String ARG_RETURN_TOP = "returnTop";
    public static final String ARG_NUMBER_OF_VERTICES = "numberOfVertices";
    public static final String ARG_NUMBER_OF_EDGES = "numberOfEdges";
    public static final String ARG_NUMBER_OF_LINES = "numberOfLines";
    public static final String ARG_FILE_SIZE = "fileSize";
    public static final String ARG_ELAPSED_TIME = "elapsedTime";
    public static final String ARG_TOP_VERTICES = "topVertices";
    public static final String ARG_NUMBER_OF_CLUSTERS = "numberOfClusters";
    public static final String ARG_SRC_COLUMN_NAME = "srcColumnName";
    public static final String ARG_DST_COLUMN_NAME = "dstColumnName";
    public static final String ARG_WEIGHT_COLUMN_NAME = "weightColumnName";
    public static final String ARG_DIRECTED = "directed";
    public static final String ARG_OVERWRITE = "overwrite";
    public static final String ARG_EDGE_CSV_FILE_PATH = "edgeCSVFilePath";
    public static final String ARG_EDGE_SRC_COLUMN = "edgeSrcColumn";
    public static final String ARG_EDGE_DST_COLUMN = "edgeDstColumn";
    public static final String ARG_EDGE_PROPERTY_COLUMNS = "edgePropertyColumns";
    public static final String ARG_OUTPUT_GRAPH_TYPE = "outputGraphType";
    public static final String ARG_OUTPUT_GRAPH_CONF_FILE = "outputGraphConfFile";
    public static final String ARG_RUNMODE = "runMode";

    public void GopCSVFileToGraph(Map<String, Object> arguments) throws Exception {
        String edgeCSVFilePath = (String)arguments.get(ARG_EDGE_CSV_FILE_PATH);
        String edgeSrcColumn = (String) arguments.get(ARG_EDGE_SRC_COLUMN);
        String edgeDstColumn = (String) arguments.get(ARG_EDGE_DST_COLUMN);
        List<String> edgePropertyColumns = (List<String>) arguments.get(ARG_EDGE_PROPERTY_COLUMNS);
        Boolean directed = (Boolean)arguments.get(ARG_DIRECTED);
        String outputGraphType = (String)arguments.get(ARG_OUTPUT_GRAPH_TYPE);
        String outputGraphConfFile = (String)arguments.get(ARG_OUTPUT_GRAPH_CONF_FILE);
        String runMode = (String)arguments.get(ARG_RUNMODE);
        if (outputGraphType.equals(TYPE_GRYOFILE) && runMode.equals(MODE_SINGLE_CLIENT) ) {
            SerialGraphOperatorsImpl.GopCSVFileToGryoGraphSingleClient(edgeCSVFilePath, edgeSrcColumn, edgeDstColumn,
                    edgePropertyColumns, directed, outputGraphConfFile);

        } else if (outputGraphType.equals(TYPE_TINKERPOPDB_GS) && runMode.equals(MODE_SINGLE_CLIENT)) {

        } else {
            throw new UnsupportedOperationException("No implementation for graph type:" + outputGraphType);
        }
    }


}

