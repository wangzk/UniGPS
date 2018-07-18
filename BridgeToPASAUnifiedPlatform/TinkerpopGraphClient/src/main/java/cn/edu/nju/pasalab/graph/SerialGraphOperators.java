package cn.edu.nju.pasalab.graph;

import cn.edu.nju.pasalab.graph.impl.SerialGraphOperatorsImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implement the serial graph operators.
 * The output results are stored in a map. The key of the map is the argument name. The value of the map is the actual data.
 * @author wzk
 */
public class SerialGraphOperators extends GraphOperators{

    public static Map<String, Object> GopCSVFileToTinkerGraphSerial(Map<String, Object> arguments) throws Exception {
        String csvFile, graphName, gremlinServerConfFile, srcColumnName, dstColumnName, weightColumnName;
        boolean directed, overwrite;
        try {
            csvFile = (String)arguments.get(ARG_CSV_FILE);
            graphName = (String)arguments.get(ARG_GRAPH_NAME);
            gremlinServerConfFile = (String)arguments.get(ARG_GREMLIN_SERVER_CONF_FILE);
            srcColumnName = (String)arguments.get(ARG_SRC_COLUMN_NAME);
            dstColumnName = (String)arguments.get(ARG_DST_COLUMN_NAME);
            weightColumnName = (String)arguments.get(ARG_WEIGHT_COLUMN_NAME);
            directed = (Boolean)arguments.getOrDefault(ARG_DIRECTED, Boolean.TRUE);
            overwrite = (Boolean)arguments.getOrDefault(ARG_OVERWRITE, Boolean.TRUE);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while converting arguments: " + e);
        }

        try {
            return
                    SerialGraphOperatorsImpl.GopCSVFileToTinkerGraphSerial(csvFile, graphName, gremlinServerConfFile,
                            srcColumnName, dstColumnName, weightColumnName,
                            directed, overwrite);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error while running the GopCSVFileToTinkerGraphSerial operator:" + e.getMessage());
        }
    }

    public static Map<String, Object> GopVertexPropertiesToCSVFileSerial(Map<String, Object> arguments) throws Exception {
        String csvFile, graphName, gremlinServerConfFile;
        List<String> properties = new ArrayList<>();
        boolean overwrite;
        try {
            csvFile = (String)arguments.get(ARG_CSV_FILE);
            graphName = (String)arguments.get(ARG_GRAPH_NAME);
            gremlinServerConfFile = (String)arguments.get(ARG_GREMLIN_SERVER_CONF_FILE);
            properties = (List<String >)arguments.getOrDefault(ARG_PROPERTIES, properties);
            overwrite = (Boolean)arguments.getOrDefault(ARG_OVERWRITE, Boolean.TRUE);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while converting arguments: " + e);
        }

        try {
            return
                    SerialGraphOperatorsImpl.GopVertexPropertiesToCSVFileSerial(graphName, gremlinServerConfFile,
                            csvFile, properties, overwrite);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error while running the GopCSVFileToTinkerGraphSerial operator:" + e.getMessage());
        }
    }

    public static Map<String, Object> GopPageRankWithGraphComputerSerial(Map<String, Object> arguments) throws Exception {
        String graphName, gremlinServerConfFile, resultPropertyName;
        Integer returnTop = new Integer(0);
        try {
            graphName = (String)arguments.get(ARG_GRAPH_NAME);
            gremlinServerConfFile = (String)arguments.get(ARG_GREMLIN_SERVER_CONF_FILE);
            resultPropertyName = (String)arguments.get(ARG_RESULT_PROPERTY_NAME);
            returnTop = (Integer) arguments.getOrDefault(ARG_RETURN_TOP, new Integer(0));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while converting arguments: " + e);
        }

        try {
            return
                    SerialGraphOperatorsImpl.GopPageRankWithGraphComputerSerial(graphName,
                            gremlinServerConfFile, resultPropertyName, returnTop);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error while running the GopCSVFileToTinkerGraphSerial operator:" + e.getMessage());
        }
    }

    public static Map<String, Object> GopPeerPressureWithGraphComputerSerial(Map<String, Object> arguments) throws Exception {
        String graphName, gremlinServerConfFile, resultPropertyName;
        try {
            graphName = (String)arguments.get(ARG_GRAPH_NAME);
            gremlinServerConfFile = (String)arguments.get(ARG_GREMLIN_SERVER_CONF_FILE);
            resultPropertyName = (String)arguments.get(ARG_RESULT_PROPERTY_NAME);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while converting arguments: " + e);
        }

        try {
            return
                    SerialGraphOperatorsImpl.GopPeerPressureWithGraphComputerSerial(graphName,
                            gremlinServerConfFile, resultPropertyName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error while running the GopCSVFileToTinkerGraphSerial operator:" + e.getMessage());
        }
    }
}
