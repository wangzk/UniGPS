package cn.edu.nju.pasalab.graph.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import cn.edu.nju.pasalab.graph.impl.util.DataBaseUtils;

import java.io.Serializable;
import java.util.Map;

import cn.edu.nju.pasalab.graphx.GraphDBGraphXConverter;

public class DemoGremlinNeo4j {
    public static void main(String[] args) throws Exception {

        String neo4jConfPath = "./conf/database/Neo4j.conf";
        Graph graph = DataBaseUtils.openDB(neo4jConfPath);
        Transaction transaction = graph.tx();
        // use Graph API to create, update and delete Vertices and Edges
        graph.io(IoCore.graphson()).readGraph("/home/lijunhong/graphxtosontest/directed.csv.graph/test.json");
        transaction.commit();

        SparkConf conf = new SparkConf().setMaster("local").setAppName("gremlin neo4j");
        SparkContext sc = new SparkContext(conf);

        org.apache.spark.graphx.Graph<Map<String, Serializable>,
                Map<String, Serializable>> graphxTest =
                GraphDBGraphXConverter.GraphDBToGraphX(graph.traversal(),sc);

        GraphDBGraphXConverter.GraphXToGraphDB(neo4jConfPath,graphxTest);

    }
}
