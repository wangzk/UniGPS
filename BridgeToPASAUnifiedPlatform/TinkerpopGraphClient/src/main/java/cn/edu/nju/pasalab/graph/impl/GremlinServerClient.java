package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.MyEdge;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;

public class GremlinServerClient {

    private Cluster cluster;
    private Client client;
    private String labelName;
    private String vertexLabel;
    private String edgeLabel;

    public GremlinServerClient(File confFile, String graphName) throws FileNotFoundException {
        GryoMapper.Builder kryo = GryoMapper.build();
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        cluster = Cluster.build(confFile)
                .serializer(serializer)
                .create();
        client = cluster.connect();
        client.submit("graph.tx().rollback(); g.tx().rollback();[]").one();
        this.labelName = graphName;
        this.vertexLabel = this.labelName + "-V";
        this.edgeLabel = this.labelName + "-E";
    }

    public GremlinServerClient(String address, int port, String graphName) {
        GryoMapper.Builder kryo = GryoMapper.build();
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        cluster = Cluster.build().addContactPoint(address).port(port)
                .serializer(serializer)
                .create();
        client = cluster.connect();
        client.submit("graph.tx().rollback(); g.tx().rollback();[]").one();
        this.labelName = graphName;
        this.vertexLabel = this.labelName + "-V";
        this.edgeLabel = this.labelName + "-E";
    }

    public void close() throws IOException {
        client.close();
        cluster.close();
    }

    public void commit() {
        client.submit("g.tx().commit();[]").one();
    }

    public GraphTraversalSource getRemoteTraversal() {
        GraphTraversalSource g = EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(this.cluster));
        return g;
    }

    private ResultSet submitCommand(String command) {
        System.out.println(":> " + command);
        ResultSet results = client.submit(command);
        commit();
        return results;
    }

    public long getNumOfV() {
        String command = String.format("g.V().hasLabel('%s').count()", vertexLabel);
        return submitCommand(command).one().getLong();
    }

    public long getNumofE() {
        String command = String.format("g.E().hasLabel('%s').count()", edgeLabel);
        return submitCommand(command).one().getLong();
    }

    public void clearVertices() {
        String command = String.format("g.V().hasLabel('%s').drop()", vertexLabel);
        submitCommand(command).one();
    }

    public void clearEdges() {
        submitCommand(String.format("g.E().hasLabel('%s').drop()", edgeLabel)).one();
    }

    public void createVerticesInBatch(Collection<String> vnames) throws ExecutionException, InterruptedException {
        String template = ".V().hasLabel('%s').has('name', '%s').fold().coalesce(unfold(),addV('%s').property('name', '%s'))";
        StringBuilder commands = new StringBuilder();
        commands.append("g");
        vnames.stream().forEach(vname -> commands.append(String.format(template, vertexLabel, vname, vertexLabel, vname)));
        commands.append(".iterate();[];");
        submitCommand(commands.toString()).all().get();
    }

    public void addE(String src, String dst) throws ExecutionException, InterruptedException {
        MyEdge e = new MyEdge(src, dst);
        ArrayList<MyEdge> list = new ArrayList<>();
        list.add(e);
        addE(list);
    }

    public void addE(String src, String dst, double weight) throws ExecutionException, InterruptedException {
        MyEdge e = new MyEdge(src, dst, weight);
        ArrayList<MyEdge> list = new ArrayList<>();
        list.add(e);
        addE(list);
    }

    public void addE(Collection<MyEdge> edges) throws ExecutionException, InterruptedException {
        addE(edges, false);
    }

    public void addE(Collection<MyEdge> edges, boolean check) throws ExecutionException, InterruptedException {
        if (check) {
            Set<String> vids = new HashSet<>();
            for (MyEdge e : edges) {
                vids.add(e.getSrc());
                vids.add(e.getDst());
            }
            createVerticesInBatch(vids);
        }
        String addEdgeTemplate = ".V().has(label, '%s').has('name','%s')" +
                ".addE('%s').to(V().has(label, '%s').has('name','%s'))" +
                ".property('weight', (Double)%f)";
        StringBuilder command = new StringBuilder("g");
        for(MyEdge e:edges) {
            command.append(String.format(addEdgeTemplate, vertexLabel, e.getSrc(),
                    edgeLabel,
                    vertexLabel, e.getDst(), e.getWeight()));
        }
        command.append(".iterate()");
        submitCommand(command.toString()).one();
    }

    public List<Long> getVertexIDs() {
        String command = String.format("g.V().hasLabel('%s').id()", vertexLabel);
        ResultSet resultSet = client.submit(command);
        return resultSet.stream().map(result -> result.getLong()).collect(Collectors.toList());
    }

    public void processVertexProperties(Consumer<Map<String, Object>> consumerAction) throws Exception {
        GraphTraversalSource g = getRemoteTraversal();
        GraphTraversal<Vertex, Map<String, Object>> t = g.V().hasLabel(vertexLabel).valueMap();
        t.toStream().forEach(consumerAction);
        g.close();
    }

    public List<Map<String, Object>> getVertexProperties() throws Exception {
        String command = String.format("g.V().hasLabel('%s')", vertexLabel);
        GraphTraversalSource g = getRemoteTraversal();
        GraphTraversal<Vertex, Map<String, Object>> t = g.V().hasLabel(vertexLabel).valueMap();
        List<Map<String, Object>> results = t.toList();
        g.close();
        return results;
    }

    public Map<String, Double> pageRank(String resultPropertyName, int returnTop) throws Exception {
        System.out.println("Start PageRank computation...");
        String template = "g2=g.withComputer();"
                + "g2.V().hasLabel('%s').pageRank().by(outE('%s')).by('%s').iterate();g2.tx().commit();g2.close();[]";
        String command = String.format(template, vertexLabel, edgeLabel, resultPropertyName);
        submitCommand(command).one();
        System.out.println("Done!");
        Map<String, Double> ranks = new HashMap<>();
        if (returnTop > 0) {
            GraphTraversalSource g = getRemoteTraversal();
            GraphTraversal topResults = g.V().hasLabel(vertexLabel).order().by(resultPropertyName, decr).limit(returnTop).valueMap("name", resultPropertyName);
            while (topResults.hasNext()) {
                Map<String, Object> vMap = (Map<String, Object>)topResults.next();
                String vertex = ((List<String>)vMap.get("name")).get(0);
                Double pageRank = ((List<Double>)vMap.get(resultPropertyName)).get(0);
                ranks.put(vertex, pageRank);
            }
            g.close();
            System.out.println(topResults);
        }
        commit();
        return ranks;
    }

    public long peerPressure(String resultPropertyName) {
        System.out.println("Start Peer Pressure computation...");
        String template = "g.withComputer().V().hasLabel('%s').peerPressure().by('%s').iterate();[]";
        String command = String.format(template, vertexLabel, resultPropertyName);
        submitCommand(command).one();
        System.out.println("Done!");
        String countClusterNumCommand =
                String.format("g.withComputer().V().hasLabel('%s').values('%s').dedup().count()", vertexLabel, resultPropertyName);
        long clusterCount = submitCommand(countClusterNumCommand).one().getLong();
        commit();
        return clusterCount;
    }


    public static void main(String args[]) throws Exception {
        //GremlinServerClient loader = new GremlinServerClient("localhost", 8182, "testG");
        GremlinServerClient loader = new GremlinServerClient(new File("/home/wzk/workspace/PASAGraphProcessingSystem/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/test-remote.yaml"), "testG");
        loader.clearVertices();
        System.out.println(loader.getNumOfV());
        List<String> vnames = new ArrayList<String>();
        vnames.add("a");vnames.add("b");vnames.add("c"); vnames.add("d"); vnames.add("e");
        loader.createVerticesInBatch(vnames);
        System.out.println(loader.getNumOfV());
        loader.clearEdges();
        System.out.println(loader.getNumofE());
        List<MyEdge> edges = new ArrayList<>();
        edges.add(new MyEdge("a","b", 1.0));
        edges.add(new MyEdge("b","c", 2.0));
        edges.add(new MyEdge("c","a", 3.0));
        loader.addE(edges);
        System.out.println(loader.getNumofE());
        loader.close();
    }


}
