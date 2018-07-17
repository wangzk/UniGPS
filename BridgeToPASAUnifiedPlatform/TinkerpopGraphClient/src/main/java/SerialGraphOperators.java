import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Implement the serial graph operators.
 * The output results are stored in a map. The key of the map is the argument name. The value of the map is the actual data.
 * @author wzk
 */
public class SerialGraphOperators extends GraphOperators{

    Map<String, Object> GopCSVFileToTinkerGraphSerial(Map<String, Object> arguments) throws Exception {
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

    Map<String, Object> GopCSVFileToTinkerGraphSerial(String csvFile,
                                                      String graphName,
                                                      String gremlinServerConfFile,
                                                      boolean deleteExistingGraph) throws Exception {
        return null;
    }

    Map<String, Object> GopTinkerGraphToCSVFileSerial(String graphName,
                                                      String gremlinServerConfFile,
                                                      String ccsFile,
                                                      boolean deleteExistingFile) {
        return null;
    }

    Map<String, Object> GopPageRankWithGraphComputerSerial(String graphName,
                                                           String gremlinServerConfFile,
                                                           String resultPropertyName,
                                                           int returnTop){
        return null;
    }



}
