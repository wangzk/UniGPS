package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters;

import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

public class CommonSerial {
    public static Logger logger = Logger.getLogger(CommonSerial.class);

    public static Vertex getOrCreateVertex(Graph g, String name) {
        return g.traversal().V().has("name", name).fold().coalesce(unfold(),
                        addV("v").property("name", name)).next();
    }

    public static Edge addEdge(Graph graph, Vertex src, Vertex dst,
                                CSVRecord record,
                                CSVUtils.CSVSchema schema,
                                List<String> edgePropertyColumns) {
        Edge e = graph.traversal().addE("e").from(src).to(dst).next();
        for (String propertyName:edgePropertyColumns) {
            CSVUtils.CSVSchema.PropertyType type = schema.getColumnType().get(propertyName);
            int propertyIndex = schema.getColumnIndex().get(propertyName);
            switch (type) {
                case DOUBLE:
                    e.property(propertyName, Double.valueOf(record.get(propertyIndex)));
                    break;
                case INT:
                    e.property(propertyName, Integer.valueOf(record.get(propertyIndex)));
                    break;
                case STRING:
                    e.property(propertyName, record.get(propertyIndex));
                    break;
            }
        }
        return e;
    }
    public static TinkerGraph loadGraphSONToTinkerGraph(String inputGraphPath) throws IOException {
        ///// Input
        FileSystem fs = HDFSUtils.getFS(inputGraphPath);
        TinkerGraph graph = TinkerGraph.open();
        logger.info("Start to load graph...");
        try(final InputStream graphFileStream = fs.open(new Path(inputGraphPath))) {
            graph.io(graphson()).reader().create().readGraph(graphFileStream, graph);
        }
        System.out.println("Load gryo graph into memory done!");
        System.out.println("|V|=" + graph.traversal().V().count().next()
                + "\n|E|=" + graph.traversal().E().count().next());
        //System.err.println(graph.traversal().V().valueMap(true).next());
        //System.err.println(graph.traversal().E().valueMap(true).next());
        return graph;
    }
}
