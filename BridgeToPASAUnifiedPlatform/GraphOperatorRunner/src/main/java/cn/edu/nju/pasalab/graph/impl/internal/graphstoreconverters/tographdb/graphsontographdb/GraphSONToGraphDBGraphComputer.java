package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.graphsontographdb;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.util.Iterator;
import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH;

public class GraphSONToGraphDBGraphComputer {
    /*public static void converter(Map<String, String> arguments) throws Exception {
        //////////// Arguments
        // For GraphSON file, the conf path is the graph file path
        String inputGraphFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        String graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);
        String dbConfFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
        String outputGraphType = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_TYPE);

        ////////// Input graph
        org.apache.commons.configuration.Configuration inputGraphConf = new BaseConfiguration();
        inputGraphConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphFilePath);
        inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath);

        ////////// Output vertex data
        CommonGraphComputer.ManageSparkContexts manageSparkContexts = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB");
        SparkContext sc = manageSparkContexts.getSc();
        JavaSparkContext jsc = manageSparkContexts.getJsc();

        InputRDD graphRDDInput = new InputFormatRDD();
        JavaPairRDD<Object, VertexWritable> vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc);

        ////////// First insert vertex in the GraphDB
        vertexWritableJavaPairRDD.foreachPartition(tuple2Iterator -> {
            Graph graphdb = CommonSerial.createGraph(dbConfFilePath,outputGraphType);

            while (tuple2Iterator.hasNext()){
                StarGraph.StarVertex v = tuple2Iterator.next()._2.get();
                StarGraph g = StarGraph.of(v);
                GraphTraversal dbg = graphdb.traversal().addV(v.label());
                dbg.property(T.id,v.id());
                Iterator<VertexProperty<Object>> propiter = v.properties();
                while (propiter.hasNext()){
                    VertexProperty attr = propiter.next();
                    dbg.property(attr.key(),attr.value());
                }
            }
            CommonSerial.safeCommit(graphdb.traversal());
        });

        ////////// Handle edge data
        JavaRDD<Edge> edgelistRDD = vertexWritableJavaPairRDD.flatMap(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            return g.traversal().V(v.id()).outE().toList().iterator();
        });

        edgelistRDD.foreachPartition(edgeIterator -> {
            Graph graphdb = CommonSerial.createGraph(dbConfFilePath,outputGraphType);

            while (edgeIterator.hasNext()){
                Edge e = edgeIterator.next();
                GraphTraversal dbg = graphdb.traversal().V(e.inVertex()).as("a")
                        .V(e.outVertex()).as("b").addE(e.label()).from("a").to("b");
                dbg.property(T.id,e.id());
                while (e.properties().hasNext()){
                    Property<Object> attr = e.properties().next();
                    dbg.property(attr.key(),attr.value());
                }
                //dbg.next();
            }
            CommonSerial.safeCommit(graphdb.traversal());
        });

        System.out.println("Finished load the GraphSON to GraphDB,the total edge count is " + edgelistRDD.count());

    }*/
}
