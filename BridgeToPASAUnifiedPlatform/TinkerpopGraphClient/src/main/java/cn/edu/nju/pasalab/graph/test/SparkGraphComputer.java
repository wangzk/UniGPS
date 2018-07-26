package cn.edu.nju.pasalab.graph.test;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;

public class SparkGraphComputer {

    private static void testNeo4jGraph() throws Exception {
        Graph nGraph = GraphFactory.open("outdb.properties");
        System.out.println(nGraph.traversal().V().count().next());
        nGraph.close();
    }

    private static void clearDB() throws Exception {
        Graph graph = GraphFactory.open("outdb.properties");
        boolean supportsTransactions = graph.features().graph().supportsTransactions();
        graph.traversal().V().drop();
        if (supportsTransactions) {
            graph.tx().commit();
        }
        graph.close();
    }

    public static void main(String args[]) throws Exception {
        clearDB();
       // System.exit(0);
        Graph graph = GraphFactory.open("SparkGraphComputerConf.properties");
        graph.configuration().setProperty("gremlin.hadoop.inputLocation", "outg");
        graph.configuration().setProperty("gremlin.hadoop.outputLocation", "testout");
        //graph.configuration().getKeys().forEachRemaining(key -> System.out.println(graph.configuration().getString(key)));
        GraphTraversalSource g = graph.traversal().withComputer(org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer.class);
        System.out.println(graph);
        System.out.println(g.V().valueMap(true).toList());
        System.out.println(g.V().count().next());
        g.V().peerPressure().by("cluster").valueMap(true).forEachRemaining(v -> System.out.println(v));

        // Bulk loading
        BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build().keepOriginalIds(false)
                .writeGraph("outdb.properties").create(graph);
        ComputerResult result = graph.compute(org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer.class).workers(2)
                .program(blvp).submit().get();
        System.out.println(result.memory());
        testNeo4jGraph();
    }
}
