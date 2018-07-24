package cn.edu.nju.pasalab.graph.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Test the usage of Gryo graph format
 */
public class TestGryoFormat {

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

}
