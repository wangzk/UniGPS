package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphdbtographson.GraphDBToGraphSONSerial;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd.GraphDBToGraphRDD;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphDBToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.util.DBClient.client.IClient;
import cn.edu.nju.pasalab.graph.util.DBClient.factory.Neo4jClientFactory;
import cn.edu.nju.pasalab.graph.util.DBClient.factory.OrientDBClientFactory;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.GraphDBGraphXConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.Serializable;
import java.util.Map;

public class DemoGremlinDB {
    public static void main(String[] args) throws Exception {

        /*String confPath = "./conf/database/orientdb.conf";
        OrientDBClientFactory factory = new OrientDBClientFactory();
        IClient db = factory.createClient(confPath);
        Graph graph = db.openDB();*/

        String confPath = "./conf/database/Neo4j.conf";
        Neo4jClientFactory factory = new Neo4jClientFactory();
        IClient db = factory.createClient(confPath);
        Graph graph = db.openDB();

        /*GraphDBToGraphSONSerial.converter(confPath ,Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/serialdbtoson.json");*/

        Transaction transaction = graph.tx();
        // use Graph API to create, update and delete Vertices and Edges
        graph.io(IoCore.graphson()).readGraph("/home/lijunhong/graphxtosontest/directed.csv.graph/test.json");
        //graph.io(IoCore.graphson()).readGraph("/home/lijunhong/graphxtosontest/tmp_181212173254/part-r-00000");
        transaction.commit();


        /*SparkConf conf = new SparkConf().setMaster("local").setAppName("gremlin neo4j");
        SparkContext sc = new SparkContext(conf);

        GraphDBToGraphSONGraphX.converter(graph.traversal(),sc,confPath,
                "/home/lijunhong/graphxtosontest/dbtoson.json");*/
/*        org.apache.spark.graphx.Graph<Map<String, Serializable>,
                Map<String, Serializable>> graphxTest  = GraphDBToGraphRDD.converter(graph.traversal(),sc,confPath);

        System.out.println(graphxTest.edges().count());*/


        /*for(Edge line:graphxTest.edges().count()){
            System.out.println("* "+line.srcId());
        }*/
        //GraphDBGraphXConverter.testHadoop(graph.traversal(),sc,confPath);

        /*org.apache.spark.graphx.Graph<Map<String, Serializable>,
                Map<String, Serializable>> graphxTest =
                GraphDBGraphXConverter.GraphDBToGraphX(graph.traversal(),sc);

        GraphDBGraphXConverter.GraphXToGraphDB(confPath,graphxTest,false);*/


    }
}