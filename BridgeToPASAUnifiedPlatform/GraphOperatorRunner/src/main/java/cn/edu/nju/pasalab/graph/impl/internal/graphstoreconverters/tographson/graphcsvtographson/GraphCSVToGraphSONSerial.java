package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson;

import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial.*;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

public class GraphCSVToGraphSONSerial {

    public static void converter(Map<String, String> arguments) throws Exception {
        ////////// Arguments
        String edgeCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_EDGE_CSV_FILE_PATH);
        String edgeSrcColumn = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_SRC_COLUMN);
        String edgeDstColumn = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_DST_COLUMN);
        Boolean directed = Boolean.valueOf(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_DIRECTED));
        List<String> edgePropertyColumns =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_PROPERTY_COLUMNS));
        // For GraphSON, the conf file path is also the output file path.
        String outputFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
        // For Hadoop graph computer, the graph computer file is the run mode conf file

        ///////////// Store to the graph
        Path edgeCSVFileHDFSPath = new Path(edgeCSVFilePath);
        Path dataFilePath = new Path(edgeCSVFileHDFSPath, "data.csv");
        Path schemaFilePath = new Path(edgeCSVFileHDFSPath, "schema");

        ///////// CSV File
        FileSystem fs = HDFSUtils.getFS(edgeCSVFilePath);
        FSDataInputStream csvFileStream = fs.open(dataFilePath);
        Reader in = new InputStreamReader(csvFileStream);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
        CSVUtils.CSVSchema schema = new CSVUtils.CSVSchema(schemaFilePath);
        int srcColumnIndex = schema.getColumnIndex().get(edgeSrcColumn);
        int dstColumnIndex = schema.getColumnIndex().get(edgeDstColumn);


        ///////// TinkerGraph
        Graph graph = TinkerGraph.open();
        Map<String, Vertex> nameToVertex = new HashMap<>();
        AtomicLong counter = new AtomicLong();

        records.forEach(record -> {
            String srcVName = record.get(srcColumnIndex);
            String dstVName = record.get(dstColumnIndex);
            Vertex src = getOrCreateVertex(graph, srcVName);
            Vertex dst = getOrCreateVertex(graph, dstVName);
            addEdge(graph, src, dst, record, schema, edgePropertyColumns);
            if (!directed) {
                addEdge(graph, dst, src, record, schema, edgePropertyColumns);
            }
            if (counter.addAndGet(1L) % 1000 == 0) {
                logger.info("\rreading csv lines... " + counter.get());
            }
        });

        logger.info("Load to memory, done!. ");
        logger.info("|V|=" + graph.traversal().V().count().next()
                + "\n|E|=" + graph.traversal().E().count().next());

        ///// Output file
        FSDataOutputStream outputStream = fs.create(new Path(outputFilePath), true);
        graph.io(graphson()).writer().create().writeGraph(outputStream, graph);
        outputStream.close();
        logger.info("Save the GraphSON file at: " + new Path(outputFilePath).toUri());
    }
}
