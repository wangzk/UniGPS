package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.impl.DBClient.client.IClient;
import cn.edu.nju.pasalab.graph.impl.DBClient.factory.OrientDBClientFactory;
import cn.edu.nju.pasalab.graphx.GraphDBGraphXConverter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.Serializable;
import java.util.*;

public class DemoGremlinDB {
    public static void main(String[] args) throws Exception {

        String confPath = "./conf/database/orientdb.conf";
        OrientDBClientFactory factory = new OrientDBClientFactory();
        IClient db = factory.createClient(confPath);
        Graph graph = db.openDB();

        /*String confPath = "./conf/database/Neo4j.conf";
        Neo4jClientFactory factory = new Neo4jClientFactory();
        IClient db = factory.createClient(confPath);
        Graph graph = db.openDB();*/

        Transaction transaction = graph.tx();
        // use Graph API to create, update and delete Vertices and Edges
        graph.io(IoCore.graphson()).readGraph("/home/lijunhong/graphxtosontest/directed.csv.graph/test.json");
        transaction.commit();

        SparkConf conf = new SparkConf().setMaster("local").setAppName("gremlin neo4j");
        SparkContext sc = new SparkContext(conf);

        org.apache.spark.graphx.Graph<Map<String, Serializable>,
                Map<String, Serializable>> graphxTest =
                GraphDBGraphXConverter.GraphDBToGraphX(graph.traversal(),sc);

        GraphDBGraphXConverter.GraphXToGraphDB(confPath,graphxTest,false);

    }
}