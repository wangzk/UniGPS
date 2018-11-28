package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.graphsontographdb;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.util.DBClient.client.IClient;
import cn.edu.nju.pasalab.graph.util.DBClient.factory.Neo4jClientFactory;
import cn.edu.nju.pasalab.graph.util.DBClient.factory.OrientDBClientFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial.logger;

public class GraphSONToGraphDBSerial {
    public static void converter(Map<String, String> arguments) throws Exception {
        ////////// Arguments
        String graphSONFilePath = arguments.get(Constants.ARG_INPUT_GRAPH_CONF_FILE)+"/part-r-00000";

        // For GraphDB, the conf file path is the configuration of the database.
        String outputConfPath = arguments.get(Constants.ARG_OUTPUT_GRAPH_CONF_FILE);

        String outputGraphType = arguments.get(Constants.ARG_OUTPUT_GRAPH_TYPE);

        Graph graph = createGraph(outputGraphType,outputConfPath);

        Transaction transaction = graph.tx();
        graph.io(IoCore.graphson()).readGraph(graphSONFilePath);
        transaction.commit();
        logger.info("Save the graphson to the GraphDB:" + outputGraphType);
    }

    public static Graph createGraph(String type, String confPath) throws Exception {
        if (type.equals(Constants.GRAPHTYPE_GRAPHDB_ORIENTDB)){
            OrientDBClientFactory factory = new OrientDBClientFactory();
            IClient db = factory.createClient(confPath);
            return db.openDB();
        } else if (type.equals(Constants.GRAPHTYPE_GRAPHDB_NEO4J)) {
            Neo4jClientFactory factory = new Neo4jClientFactory();
            IClient db = factory.createClient(confPath);
            return db.openDB();
        } else {
            throw new UnsupportedOperationException("No implementation for graphDB type:" + type);
        }
    }
}
