package cn.edu.nju.pasalab.graph;

import cn.edu.nju.pasalab.graph.impl.SerialGraphOperatorsImpl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Implement the serial graph operators.
 * The output results are stored in a map. The key of the map is the argument name. The value of the map is the actual data.
 * @author wzk
 */
public class SerialGraphOperators extends GraphOperators{

    public static Map<String, Object> GopCSVFileToTinkerGraphSerial(Map<String, Object> arguments) throws Exception {
        String csvFile, graphName, gremlinServerConfFile;
        boolean deleteExistingGraph;
        try {
            csvFile = (String)arguments.get(ARG_CSV_FILE);
            graphName = (String)arguments.get(ARG_GRAPH_NAME);
            gremlinServerConfFile = (String)arguments.get(ARG_GREMLIN_SERVER_CONF_FILE);
            deleteExistingGraph = (Boolean)arguments.get(ARG_DELETE_EXISTING_GRAPH);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while converting arguments: " + e);
        }

        try {
            return GopCSVFileToTinkerGraphSerial(csvFile, graphName, gremlinServerConfFile, deleteExistingGraph);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error while running the GopCSVFileToTinkerGraphSerial operator:" + e.getMessage());
        }
    }

    public static Map<String, Object> GopCSVFileToTinkerGraphSerial(String csvFile,
                                                      String graphName,
                                                      String gremlinServerConfFile,
                                                      boolean deleteExistingGraph) throws Exception {
        return SerialGraphOperatorsImpl.GopCSVFileToTinkerGraphSerial(csvFile,
                graphName, gremlinServerConfFile, deleteExistingGraph);
    }

    public static Map<String, Object> GopVertexTableToCSVFileSerial(String graphName,
                                                      String gremlinServerConfFile,
                                                      String csvFile,
                                                      List<String> properties,
                                                      boolean deleteExistingFile) throws Exception {
        return
                SerialGraphOperatorsImpl.GopVertexTableToCSVFileSerial(graphName, gremlinServerConfFile, csvFile, properties, deleteExistingFile);
    }


    public static Map<String, Object> GopPageRankWithGraphComputerSerial(String graphName,
                                                           String gremlinServerConfFile,
                                                           String resultPropertyName,
                                                           int returnTop) throws Exception {
        return
                SerialGraphOperatorsImpl.GopPageRankWithGraphComputerSerial(graphName, gremlinServerConfFile, resultPropertyName, returnTop);
    }

    public static Map<String, Object> GopPeerPressureWithGraphComputerSerial(String graphName,
                                                                             String gremlinServerConfFile,
                                                                             String resultPropertyName) throws Exception {
        return SerialGraphOperatorsImpl.GopPeerPressureWithGraphComputerSerial(graphName, gremlinServerConfFile, resultPropertyName);
    }

}
