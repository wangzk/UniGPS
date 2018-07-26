package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Test the usage of Gryo graph format
 */
public class TestGryoFormat implements Serializable {

    Graph prepareDemoGraph() {
        // Part 1:
        // 1->2
        // 2->3
        // Part 2:
        // 3->1
        // 3->4
        Graph demoGraph = TinkerGraph.open();
        GraphTraversalSource g = demoGraph.traversal();
        Vertex v1 = g.addV("v").property("name", "v1").next();
        Vertex v2 = g.addV("v").property("name", "v2").next();
        Vertex v3 = g.addV("v").property("name", "v3").next();
        Vertex v4 = g.addV("v").property("name", "v4").next();
        g.addE("e").from(v1).to(v2).iterate();
        g.addE("e").from(v2).to(v3).iterate();
        g.addE("e").from(v3).to(v1).iterate();
        g.addE("e").from(v3).to(v4).iterate();
        System.out.println("Create a demo graph: " + demoGraph);
        return demoGraph;
    }

    @Test
    public void testTwoSplitsWrite() throws Exception {
        Graph demoGraph = prepareDemoGraph();
        // Part 1
        FileSystem fs = FileSystem.get(new Configuration());
        Path f1 = new Path("f1.kryo");
        Path f2 = new Path("f2.kryo");
        fs.delete(f1, true); fs.delete(f2, true);
        FSDataOutputStream f1Stream = fs.create(f1, true);
        FSDataOutputStream f2Stream = fs.create(f2, true);
        GryoWriter gryoWriter = GryoWriter.build().create();
        GraphTraversalSource g = demoGraph.traversal();
        GraphTraversal t1 = g.V().has("name", P.within("v1", "v2"));
        GraphTraversal t2 = g.V().has("name", P.within("v3", "v4"));
        //printVertexTraversal(t1);
        //printVertexTraversal(t2);
        gryoWriter.writeVertices(f1Stream, t1, Direction.OUT);
        gryoWriter.writeVertices(f2Stream, t2, Direction.OUT);
        f1Stream.close();
        f2Stream.close();
        g.close(); demoGraph.close();
    }

    @Test
    public void testTwoSplitsRead() throws Exception {
        /*
        Graph readGraph = Neo4jGraph.open("testdb");
        FileSystem fs = FileSystem.get(new Configuration());
        Path f1 = new Path("f1.kryo");
        Path f2 = new Path("f2.kryo");
        FSDataInputStream f1Stream = fs.open(f1);
        FSDataInputStream f2Stream = fs.open(f2);
        readGryoVertices(f1Stream, readGraph);
        readGryoVertices(f2Stream, readGraph);
        f1Stream.seek(0);f2Stream.seek(0);
        readGryoEdges(f1Stream, readGraph);
        readGryoEdges(f2Stream, readGraph);
       // readGryoStream(f1Stream, readGraph);
       // readGryoStream(f2Stream, readGraph);
        printGraph(readGraph);
        f1Stream.close();
        f2Stream.close();
        */
    }

    void readGryoVertices(InputStream in, Graph graphToWrite) throws IOException {
        GryoReader reader = GryoReader.build().create();
        reader.readVertices(in, vertexAttachable -> {
            return vertexAttachable.attach(Attachable.Method.getOrCreate(graphToWrite));
        }, e -> e.get(), Direction.OUT).forEachRemaining(v -> System.out.println(v));
    }

    void readGryoEdges(InputStream in, Graph graphToWrite) throws IOException {
        GryoReader reader = GryoReader.build().create();
        System.out.println("haha!");
        reader.readVertices(in, vertexAttachable -> vertexAttachable.attach(Attachable.Method.getOrCreate(graphToWrite)),
                edgeAttachable -> edgeAttachable.attach(Attachable.Method.create(graphToWrite)),
                Direction.OUT).forEachRemaining(v -> System.out.println(v));
    }

    void readGryoStream(InputStream in, Graph graphToWrite) throws IOException {
        GryoReader reader = GryoReader.build().create();
      //  System.out.println("haha");
        Iterator<Vertex> vIter = reader.readVertices(in,
                vertexAttachable -> {
                   // System.out.println("vertex attachable:" + vertexAttachable);
                    return vertexAttachable.attach(Attachable.Method.getOrCreate(graphToWrite));
                }, edgeAttachable -> {
          //  System.out.println("edge attachable:" + edgeAttachable);
            return edgeAttachable.attach(Attachable.Method.getOrCreate(graphToWrite));
                },
                Direction.OUT);
        vIter.forEachRemaining(v -> System.out.println("added v:" + v));
    }

    void printGraph(Graph graph) {
        System.out.println("Graph " + graph + ":");
        GraphTraversalSource g = graph.traversal();
        System.out.println("|V|=" + g.V().count().next() + ", |E|=" + g.E().count().next());
        g.V().valueMap(true).forEachRemaining(vmap -> System.out.println(vmap));
        g.E().forEachRemaining(emap -> System.out.println(emap));
    }

    void printVertexTraversal(Iterator<Vertex> t) {
        t.forEachRemaining(v -> System.out.println(v + "-" + v.value("name")));
        System.out.println();
    }

    @Test
    public void testGraphSONFormat() {
        BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("gremlin.graph", HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "dist/demo/jinyong.gryo");
        HadoopGraph graph = HadoopGraph.open(configuration);
        System.out.println(graph);
        System.out.println(graph.traversal().V().count().next());
        System.out.println(graph.traversal().E().count().next());
        printGraph(graph);

    }

    @Test
    public void getClassName() {
        System.out.println(HadoopGraph.class.getName());
    }

    @Test
    public void testTwoSplitWrite() throws IOException {
        File f = new File("f");
        if (f.exists()) f.delete();
        f.mkdir();
        StarGraph sg1 = StarGraph.builder().build();
        sg1.addVertex(T.id, 1 , T.label, "v", "name", "v1");
        StarGraph.StarVertex c1v1 = sg1.getStarVertex();
        c1v1.property("name", "v1");
        Vertex c1v2 = sg1.addVertex(T.id, 2,T.label, "v");
        Vertex c1v3 = sg1.addVertex(T.id, 3, T.label, "v");
        Edge c1e1 = c1v1.addEdge("e", c1v2, T.id, 1);
        Edge c1e2 = c1v1.addEdge("e", c1v3, T.id, 2);
        c1v2.addEdge("e", c1v1, T.id, 3);
        System.out.println(sg1);
        printGraph(sg1);
        StarGraph sg2 = StarGraph.builder().build();
        Vertex c2v2 = sg2.addVertex(T.id, 2, T.label, "v", "name", "v2");
        Vertex c2v1 = sg2.addVertex(T.id, 1, T.label, "v");
        Edge c2e1 = c2v2.addEdge("e", c2v1, T.id, 3);
        c2v1.addEdge("e", c2v2,T.id, 1);
        System.out.println(sg2);
        printGraph(sg2);
        StarGraph sg3 = StarGraph.builder().build();
        Vertex c3v3 = sg3.addVertex(T.id, 3, T.label, "v", "name", "v3");
        Vertex c3v1 = sg3.addVertex(T.id, 1, T.label, "v");
        c3v1.addEdge("e", c3v3, T.id, 2);

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test");
        SparkContext sc = SparkContext.getOrCreate(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        org.apache.commons.configuration.Configuration graphConf = new BaseConfiguration();
        graphConf.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph");
        graphConf.setProperty("gremlin.hadoop.graphReader", "org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat");
        graphConf.setProperty("gremlin.hadoop.graphWriter", "org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat");
        graphConf.setProperty("gremlin.hadoop.outputLocation", "testout");
        HDFSUtils.getDefaultFS().delete(new Path("testout"), true);
        ArrayList<VertexWritable> vertexLists = new ArrayList<>();
        vertexLists.add(new VertexWritable(sg1.getStarVertex()));
        vertexLists.add(new VertexWritable(sg2.getStarVertex()));
        vertexLists.add(new VertexWritable(sg3.getStarVertex()));
        JavaPairRDD<Object, VertexWritable> graphRDD = jsc.parallelize(vertexLists, 2).mapToPair(new PairFunction<VertexWritable, Object, VertexWritable>() {
            @Override
            public Tuple2<Object, VertexWritable> call(VertexWritable vertexWritable) throws Exception {
                return new Tuple2<>(NullWritable.get(), vertexWritable);
            }
        });
        OutputFormatRDD outputFormatRDD = new OutputFormatRDD();
        outputFormatRDD.writeGraphRDD(graphConf, graphRDD);
    }

    @Test
    public void testReadInSpark() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test");
        SparkContext sc = SparkContext.getOrCreate(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        org.apache.commons.configuration.Configuration graphConf = new BaseConfiguration();
        graphConf.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph");
        graphConf.setProperty("gremlin.hadoop.graphReader", "org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat");
        //graphConf.setProperty("gremlin.hadoop.inputLocation", "outg");
        graphConf.setProperty("mapreduce.input.fileinputformat.inputdir", "testout/~g");
        InputRDD graphRDDInput = new InputFormatRDD();
        JavaPairRDD<Object, VertexWritable> vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(graphConf, jsc);
        vertexWritableJavaPairRDD.foreach(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            System.out.println("v: " + v.id());
            System.out.println(IteratorUtils.stream(v.properties()).collect(Collectors.toList()));
        });
    }

    @Test
    public void hdfsGryoGraphToLocalTinkerGraph() throws ExecutionException, InterruptedException {
        org.apache.commons.configuration.Configuration readConf = new BaseConfiguration();
        readConf.setProperty("gremlin.graph", "org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph");
        readConf.setProperty("gremlin.hadoop.graphReader", "org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat");
        readConf.setProperty("gremlin.hadoop.inputLocation", "testout/~g");
        readConf.setProperty("mapreduce.input.fileinputformat.inputdir", "testout/~g");
        readConf.setProperty("spark.master", "local[1]");
        readConf.setProperty("spark.serializer","org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer");
        org.apache.commons.configuration.Configuration writeConf = new BaseConfiguration();
        writeConf.setProperty("gremlin.graph","org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
        writeConf.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
        writeConf.setProperty("gremlin.tinkergraph.graphLocation", "testout.gryo");
        new File("testout.gryo").delete();


        HadoopGraph readGraph = HadoopGraph.open(readConf);
        BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build().keepOriginalIds(true).writeGraph(writeConf).create(readGraph);
        readGraph.compute(SparkGraphComputer.class).workers(1).program(blvp).submit().get();
        TinkerGraph writeGraph = TinkerGraph.open(writeConf);
        printGraph(writeGraph);
    }

}
