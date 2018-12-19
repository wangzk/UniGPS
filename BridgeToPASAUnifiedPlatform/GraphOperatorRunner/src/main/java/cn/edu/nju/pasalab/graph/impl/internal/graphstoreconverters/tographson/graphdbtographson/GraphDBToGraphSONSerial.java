package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphdbtographson;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial.createGraph;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

public class GraphDBToGraphSONSerial {

    public static void converter(String inputConfPath, String inputGraphType, String outputFilePath) throws Exception {

        /*String inputConfPath = arguments.get(Constants.ARG_INPUT_GRAPH_CONF_FILE);
        String inputGraphType = arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);

        String outputFilePath = arguments.get(Constants.ARG_OUTPUT_GRAPH_CONF_FILE);*/
        Graph graph = createGraph(inputConfPath, inputGraphType);

        Long num = graph.traversal().V().count().next();
        FileSystem fs = HDFSUtils.getFS(outputFilePath);
        HDFSUtils.getFS(outputFilePath).delete(new Path(outputFilePath), true);

        try(final OutputStream graphFileStream = fs.create(new Path(outputFilePath))) {
            graph.io(graphson()).writer().create().writeGraph(graphFileStream, graph);
        }

    }
}
