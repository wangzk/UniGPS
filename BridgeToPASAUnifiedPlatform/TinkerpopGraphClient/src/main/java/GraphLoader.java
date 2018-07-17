import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class GraphLoader {

    private Cluster cluster;
    private Client client;
    private String labelName;

    public GraphLoader(String address, int port, String graphName) {
        GryoMapper.Builder kryo = GryoMapper.build();
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        cluster = Cluster.build().addContactPoint(address).port(port)
                .serializer(serializer)
                .create();
        client = cluster.connect().init();
        this.labelName = graphName;
    }

    public void close() throws IOException {
        client.close();
        cluster.close();
    }

    private ResultSet submitCommand(String command) {
        System.out.println(":> " + command);
        return client.submit(command);
    }

    public long getNumOfV() {
        String command = String.format("g.V().hasLabel('%s').count()", labelName);
        return submitCommand(command).one().getLong();
    }

    public long getNumofE() {
        String command = String.format("g.E().hasLabel('%s').count()", labelName);
        return submitCommand(command).one().getLong();
    }

    public void clearVertices() {
        String command = String.format("g.V().hasLabel('%s').drop()", this.labelName);
        submitCommand(command).one();
    }

    public void clearEdges() {
        submitCommand(String.format("g.E().hasLabel('%s').drop()", this.labelName)).one();
    }

    public void createVerticesInBatch(Collection<String> vnames) throws ExecutionException, InterruptedException {
        String template = ".V().hasLabel('%s').has('name', '%s').fold().coalesce(unfold(),addV('%s').property('name', '%s'))";
        StringBuilder commands = new StringBuilder();
        commands.append("g");
        vnames.stream().forEach(vname -> commands.append(String.format(template, this.labelName, vname, this.labelName, vname)));
        commands.append(".next();[];");
        submitCommand(commands.toString()).all().get();
    }

    public void addE(String src, String dst) throws ExecutionException, InterruptedException {
        Edge e = new Edge(src, dst);
        ArrayList<Edge> list = new ArrayList<>();
        list.add(e);
        addE(list);
    }

    public void addE(String src, String dst, double weight) throws ExecutionException, InterruptedException {
        Edge e = new Edge(src, dst, weight);
        ArrayList<Edge> list = new ArrayList<>();
        list.add(e);
        addE(list);
    }

    public void addE(Collection<Edge> edges) throws ExecutionException, InterruptedException {
        Set<String> vids = new HashSet<>();
        for(Edge e: edges) {
            vids.add(e.getSrc()); vids.add(e.getDst());
        }
        createVerticesInBatch(vids);
        String addEdgeTemplate = ".V().has(label, '%s').has('name','%s')" +
                ".addE('%s').to(V().has(label, '%s').has('name','%s'))" +
                ".property('weight', %f)";
        StringBuilder command = new StringBuilder("g");
        for(Edge e:edges) {
            command.append(String.format(addEdgeTemplate, this.labelName, e.getSrc(),
                    this.labelName,
                    this.labelName, e.getDst(), e.getWeight()));
        }
        command.append(".next();[]");
        submitCommand(command.toString()).one();
    }

    public static void main(String args[]) throws Exception {
        GraphLoader loader = new GraphLoader("localhost", 8182, "testG");
        loader.clearVertices();
        System.out.println(loader.getNumOfV());
        List<String> vnames = new ArrayList<String>();
        vnames.add("a");vnames.add("b");vnames.add("c"); vnames.add("d"); vnames.add("e");
        loader.createVerticesInBatch(vnames);
        System.out.println(loader.getNumOfV());
        loader.clearEdges();
        System.out.println(loader.getNumofE());
        List<Edge> edges = new ArrayList<>();
        edges.add(new Edge("a","b", 1.0));
        edges.add(new Edge("b","c", 2.0));
        edges.add(new Edge("c","a", 3.0));
        loader.addE(edges);
        System.out.println(loader.getNumofE());
        loader.close();
    }


}
