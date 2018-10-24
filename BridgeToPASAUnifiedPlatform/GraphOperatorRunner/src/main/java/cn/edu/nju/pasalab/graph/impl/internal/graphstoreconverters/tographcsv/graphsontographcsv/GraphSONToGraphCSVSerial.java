package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv;

import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        List<String> properties =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_VERTEX_PROPERTY_NAMES));
        String outputCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH);

        FileSystem fs = HDFSUtils.getFS(inputGraphPath);
        Graph graph = loadGraphSONToTinkerGraph(inputGraphPath);
        ////// CSV File
        Path outputDirPath = new Path(outputCSVFilePath);
        Path dataFilePath = new Path(outputDirPath, "data.csv");
        Path schemaFilePath = new Path(outputDirPath, "schema");
        fs.delete(outputDirPath, true);
        fs.mkdirs(outputDirPath);
        //// Write schema
        if (!properties.contains("name")) {
            properties.add(0, "name");
        }
        try(final OutputStream out = fs.create(schemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Vertex testVertex = graph.traversal().V().next();
            CSVUtils.CSVSchema schema = new CSVUtils.CSVSchema(properties, testVertex);
            writer.print(schema.toSchemaDescription());
            writer.close();
            System.out.println(schema.toSchemaDescription());
        }
        //// Write data
        try(final OutputStream out = fs.create(dataFilePath, true)) {
            PrintStream csvStream = new PrintStream(out);
            CSVPrinter printer = CSVFormat.RFC4180.print(csvStream);
            Iterator<Vertex> vertexIterator = graph.vertices();
            long count = 0;
            while (vertexIterator.hasNext()) {
                Vertex vertex = vertexIterator.next();
                List<Object> values = properties.stream().map(p -> vertex.value(p)).collect(Collectors.toList());
                printer.printRecord(values);
                count++;
            }
            printer.close();
            csvStream.close();
            logger.info("Write CSV file: " + count + " lines.");
        }
    }
}
