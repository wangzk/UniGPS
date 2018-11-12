package cn.edu.nju.pasalab.graph.impl;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class DataBaseUtils {

    private static void safeCommit(GraphTraversalSource g){
        if (g.getGraph().features().graph().supportsTransactions()) g.tx().commit();
    }

    public static Properties loadConfFromHDFS(String hdfsFilePath) throws IOException {
        Properties conf = new Properties();
        InputStream fileStream = HDFSUtils.openFile(hdfsFilePath);
        conf.load(fileStream);
        fileStream.close();
        return conf;
    }
    public static Graph openDB(String DBConfPath) throws Exception {
        Properties conf = loadConfFromHDFS(DBConfPath);
        return openDB(conf);
    }

    public static Graph openDB(Properties conf) throws Exception{
        if(conf.getProperty("type").equals("neo4j")){
            Neo4JElementIdProvider<?> vertexIdProvider = new Neo4JNativeElementIdProvider();
            Neo4JElementIdProvider<?> edgeIdProvider = new Neo4JNativeElementIdProvider();
            Driver driver = GraphDatabase.driver(conf.getProperty("uri"),
                    AuthTokens.basic(conf.getProperty("username"), conf.getProperty("password")));
            return new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider);
        }else if(conf.getProperty("type").equals("orientdb")){
            return OrientGraph.open(conf.getProperty("uri"),conf.getProperty("username"),conf.getProperty("password"));
        }else{
            throw new IOException("No implementation for graph type:" + conf.getProperty("type"));
        }
    }
    public static void deleteGraphVertices(GraphTraversalSource g){
        try {
            g.V().drop().iterate();
            safeCommit(g);
        }
        catch( OConcurrentModificationException e) {
            g.tx().rollback();

            long count = g.V().count().next();
            while (count > 0){
                g.V().limit(1000).drop().iterate();
                g.tx().commit();
                count = count - 1000;
            }
        }
    }
}