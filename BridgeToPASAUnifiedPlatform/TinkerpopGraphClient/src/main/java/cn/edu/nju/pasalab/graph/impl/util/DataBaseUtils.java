package cn.edu.nju.pasalab.graph.impl.util;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class DataBaseUtils {

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
        }else{
            throw new IOException("No implementation for graph type:" + conf.getProperty("type"));
        }
    }
}
