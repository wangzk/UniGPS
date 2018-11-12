package cn.edu.nju.pasalab.graph.impl.DBClient.client;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;


public class Neo4jClient extends AbstractClient {

    public Graph openDB() {
        Neo4JElementIdProvider<?> vertexIdProvider = new Neo4JNativeElementIdProvider();
        Neo4JElementIdProvider<?> edgeIdProvider = new Neo4JNativeElementIdProvider();
        Driver driver = GraphDatabase.driver(getParam().getProperty("uri"),
                AuthTokens.basic(getParam().getProperty("username"), getParam().getProperty("password")));
        return new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider);
    }

}
