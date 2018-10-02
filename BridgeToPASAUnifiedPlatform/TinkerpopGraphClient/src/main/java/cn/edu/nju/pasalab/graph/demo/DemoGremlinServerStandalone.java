package cn.edu.nju.pasalab.graph.demo;


import cn.edu.nju.pasalab.graph.impl.util.ConfUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.nju.pasalab.graphx.GraphTraversalGraphXConverter;

public class DemoGremlinServerStandalone {
    public static void main(String[] args) throws IOException, ConfigurationException {
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint("localhost");
        builder.port(8182);
        builder.serializer(new GryoMessageSerializerV3d0());
        Cluster cluster = builder.create();

        GraphTraversalSource g =
                EmptyGraph.instance().traversal().
                        withRemote(DriverRemoteConnection.using(cluster));

        String graphComputerConfPath = "./conf/graph-computer/SparkLocal.conf";
        SparkConf sparkConf = new SparkConf(true);
        Configuration graphComputerConf = ConfUtils.loadConfFromHDFS(graphComputerConfPath);
        ConfUtils.loadUserConfToSparkConf(sparkConf, graphComputerConf);
        sparkConf.setAppName("gremlin server graphx converter");
        SparkContext sc = new SparkContext(sparkConf);

        org.apache.spark.graphx.Graph<HashMap<String, java.io.Serializable>,
           HashMap<String, java.io.Serializable>> graphxTest =
                GraphTraversalGraphXConverter.GraphTraversalToGraphX(g,sc);

        GraphTraversalGraphXConverter.GraphXWriteToTraversal(g,graphxTest);

        cluster.close();
    }
}
