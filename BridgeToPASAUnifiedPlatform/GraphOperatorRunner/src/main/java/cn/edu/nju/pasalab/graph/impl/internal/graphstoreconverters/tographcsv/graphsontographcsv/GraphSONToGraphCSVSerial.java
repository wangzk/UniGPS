package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv;

import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial.*;

public class GraphSONToGraphCSVSerial {

    public static void converter(Map<String, String> arguments) throws Exception {
        //////////// Arguments
        // For GraphSON file, the conf path is the graph file path
        String inputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        List<String> vertexProperties =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_VERTEX_PROPERTY_NAMES));
        List<String> edgeProperties =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_PROPERTY_NAMES));
        String outputVertexCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH);
        String outputEdgeCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_EDGE_CSV_FILE_PATH);

        FileSystem fs = HDFSUtils.getFS(inputGraphPath);
        Graph graph = loadGraphSONToTinkerGraph(inputGraphPath);

        /////////// Prepare vertex output
        Path outputVertexDirPath = new Path(outputVertexCSVFilePath);
        Path vertexDataFilePath = new Path(outputVertexDirPath, "data.csv");
        Path vertexSchemaFilePath = new Path(outputVertexDirPath, "schema");
        fs.delete(outputVertexDirPath, true);
        fs.mkdirs(outputVertexDirPath);
        //// Write vertex schema
        if (!vertexProperties.contains("name")) {
            vertexProperties.add(0, "name");
        }
        try(final OutputStream out = fs.create(vertexSchemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Vertex testVertex = graph.traversal().V().next();
            CSVUtils.CSVSchema schema = new CSVUtils.CSVSchema(vertexProperties, testVertex);
            writer.print(schema.toSchemaDescription());
            writer.close();
            System.out.println(schema.toSchemaDescription());
        }
        //// Write vertex data
        try(final OutputStream out = fs.create(vertexDataFilePath, true)) {
            PrintStream csvStream = new PrintStream(out);
            CSVPrinter printer = CSVFormat.RFC4180.print(csvStream);
            Iterator<Vertex> vertexIterator = graph.vertices();
            long count = 0;
            while (vertexIterator.hasNext()) {
                Vertex vertex = vertexIterator.next();
                List<Object> values = vertexProperties.stream().map(p -> vertex.value(p)).collect(Collectors.toList());
                printer.printRecord(values);
                count++;
            }
            printer.close();
            csvStream.close();
            logger.info("Write vertex CSV file: " + count + " lines.");
        }

        /////////// Prepare edge output
        Path outputEdgeDirPath = new Path(outputEdgeCSVFilePath);
        Path edgeDataFilePath = new Path(outputEdgeDirPath, "data.csv");
        Path edgeSchemaFilePath = new Path(outputEdgeDirPath, "schema");
        fs.delete(outputEdgeDirPath, true);
        fs.mkdirs(outputEdgeDirPath);
        //// Write edge schema

        try(final OutputStream out = fs.create(edgeSchemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Edge testEdge = graph.traversal().E().next();
            CSVUtils.CSVSchema schema = new CSVUtils.CSVSchema(edgeProperties, testEdge);
            writer.print(schema.toSchemaDescription());
            writer.close();
            System.out.println(schema.toSchemaDescription());
        }
        //// Write edge data
        try(final OutputStream out = fs.create(edgeDataFilePath, true)) {
            PrintStream csvStream = new PrintStream(out);
            CSVPrinter printer = CSVFormat.RFC4180.print(csvStream);
            Iterator<Edge> edgeIterator = graph.edges();
            long count = 0;
            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                List<Object> values = edgeProperties.stream().map(p -> edge.value(p)).collect(Collectors.toList());
                printer.printRecord(values);
                count++;
            }
            printer.close();
            csvStream.close();
            logger.info("Write edge CSV file: " + count + " lines.");
        }
    }
}
