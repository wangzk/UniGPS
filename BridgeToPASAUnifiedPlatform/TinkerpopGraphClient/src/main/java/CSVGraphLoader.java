import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CSVGraphLoader {

    private Cluster cluster;
    private Client client;

    public CSVGraphLoader(String address, int port) {
        GryoMapper.Builder kryo = GryoMapper.build();
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        cluster = Cluster.build().addContactPoint(address).port(port)
                .serializer(serializer)
                .create();
        client = cluster.connect().init();
    }

    public void close() throws IOException {
        client.close();
        cluster.close();
    }

    public Client getClient() {
        return this.client;
    }

    public GraphTraversalSource getRemoteTraversal() {
        return EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(this.cluster));
    }

    public long getNumOfV() {
        String command = "g.V().count()";
        ResultSet result = client.submit(command);
        return result.one().getLong();
    }

    public long getNumofE() {
        return client.submit("g.E().count()").one().getLong();
    }

    public void clearVertices() {
        client.submit("g.V().drop()").one();
    }

    public void clearEdges() {
        client.submit("g.E().drop()").one();
    }

    public void createVerticesInBatch(List<String> vnames) throws ExecutionException, InterruptedException {
        StringBuilder commands = new StringBuilder();
        commands.append("g");
        String addVertexTemplate =
                ".V().has('name', '%s').fold().coalesce(unfold(),addV().property('name', '%s'))";
        vnames.stream().forEach(vname -> commands.append(String.format(addVertexTemplate, vname, vname)));
        commands.append(".next();[];");
        System.out.println(commands);
        client.submit(commands.toString()).all().get();
    }

    private String getOrCreateVertexCommand(String vname) {
        return String.format("V().has('name', '%s').fold().coalesce(unfold(), addV().property('name', '%s'))",
                vname, vname);
    }

    public void addE(String src, String dst, double weight) throws ExecutionException, InterruptedException {
        String template = "g.V().has('name','%s').addE('simple').to(V().has('name','%s')).next();[];";
        String command = String.format(template, src, dst);
        client.submit(command).one();
    }

    public static void main(String args[]) throws Exception {
        CSVGraphLoader loader = new CSVGraphLoader("localhost", 8182);
        loader.clearVertices();
        System.out.println(loader.getNumOfV());
        List<String> vnames = new ArrayList<String>();
        vnames.add("a");vnames.add("b");vnames.add("c"); vnames.add("d"); vnames.add("e");
        loader.createVerticesInBatch(vnames);
        System.out.println(loader.getNumOfV());
        System.out.println(loader.getNumofE());
        loader.addE("e","d", 2.0);
        System.out.println(loader.getNumofE());

        loader.close();
    }


}
