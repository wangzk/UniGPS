package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.MyEdge;
import cn.edu.nju.pasalab.graph.GraphOperators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV1d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class SerialGraphOperatorsImpl {

    static Logger logger = Logger.getLogger(SerialGraphOperatorsImpl.class);


    /**
     * Name: GopCSVFileToTinkerGraphSerial
     *
     * Action: 将一个保存在HDFS上的CSV文件，转换为一个保存在TinkerPop兼容的图数据库中的图。单机串行实现。
     *
     * Input:
     *
     *     csvFile(String). The file path to the csv file on HDFS. The CSV file should have a header and at least two columns src and dst. Each line of the CSV file represents a directed simple edge from src to dst. If there is a weight column, it will be interpreted as the weight property of the edge. The type of weight is double in the database. The src and dst will be treated as the values of the name property of the vertices in the graph.
     *     graphName(String). The name of the input graph. GraphName will be treated as the label attached on each vertex and edge in this CSV file.
     *     gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
     *     deleteExistingGraph (Boolean, optional, default = true). Whether or not delete the existing graph with the label GraphName in the database before loading the new graph in the CSV file. If DeleteExistingGraph = false and the existing graph conflicts with the new graph, exceptions will be thrown.
     *
     * Output:
     *
     *     numberOfVertices(Long). Number of vertices in the graph.
     *     numberOfEdges(Long). Number of edges in the graph.
     */
    public static Map<String, Object> GopCSVFileToTinkerGraphSerial(String csvFile,
                                                      String graphName,
                                                      String gremlinServerConfFile,
                                                      String srcColumnName, String dstColumnName, String weightColumnName,
                                                      boolean directed,
                                                      boolean overwrite) throws Exception {
        Logger logger = Logger.getLogger(SerialGraphOperatorsImpl.class);
        GremlinServerClient loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        ///////////
        if (overwrite) {
            logger.info("Start deleting the existing graph " + graphName);
            loader.clearVertices();
        }
        ///////////// Store to the graph
        FileSystem fs = HDFSUtils.getFS(csvFile);
        FSDataInputStream csvFileStream = fs.open(new Path(csvFile));
        Reader in = new InputStreamReader(csvFileStream);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        logger.info("Get CSV file header:" + ((CSVParser) records).getHeaderMap());
        ArrayList<MyEdge> tmpEdgeLists = new ArrayList<>();
        long lineCount = 0;
        for(CSVRecord record: records) {
            String src = record.get(srcColumnName);
            String dst = record.get(dstColumnName);
            if (weightColumnName != null && record.isSet(weightColumnName)) {
                Double weight = new Double(record.get("weight"));
                tmpEdgeLists.add(new MyEdge(src, dst, weight));
                if (!directed)
                    tmpEdgeLists.add(new MyEdge(dst, src, weight));
            } else {
                tmpEdgeLists.add(new MyEdge(src, dst));
                if (!directed)
                    tmpEdgeLists.add(new MyEdge(dst, src));
            }
            lineCount++;
            if (tmpEdgeLists.size() >= 50) {
                loader.addE(tmpEdgeLists, true);
                tmpEdgeLists.clear();
                System.out.println("Line count: " + lineCount);
            }
        }
        if (tmpEdgeLists.size() > 0) {
            loader.addE(tmpEdgeLists, true);
        }
        logger.info("Load graph done!");
        long numV = loader.getNumOfV();
        long numE = loader.getNumofE();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_NUMBER_OF_VERTICES, new Long(numV));
        output.put(GraphOperators.ARG_NUMBER_OF_EDGES, new Long(numE));
        loader.close();
        return output;
    }

    public static Map<String, Object> GopVertexPropertiesToCSVFileSerial(String graphName,
                                                                         String gremlinServerConfFile,
                                                                         String csvFile,
                                                                         List<String> properties,
                                                                         boolean deleteExistingFile) throws Exception {
        Logger logger = Logger.getLogger(SerialGraphOperatorsImpl.class);
        GremlinServerClient loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        //////////////
        // Prepare for output
        if (!properties.contains("name")) {
            properties.add(0, "name");
        }
        FileSystem fs = HDFSUtils.getFS(csvFile);
        FSDataOutputStream csvFileStream = fs.create(new Path(csvFile), deleteExistingFile);
        PrintStream csvStream = new PrintStream(csvFileStream);
        CSVPrinter printer = CSVFormat.DEFAULT.withHeader(properties.toArray(new String[properties.size()])).print(csvStream);
        logger.info("Start writing vertex properties...");
        //////////////
        // Vid list
        Iterator<Map<String, Object>> vProperties = loader.getVertexProperties().iterator();
        List<Object> record = new ArrayList<>();
        long numberOfLines = 0L;
        while (vProperties.hasNext()) {
            Map<String, Object> vertexProperties = vProperties.next();
            for(String p: properties) {
                record.add(((ArrayList)vertexProperties.get(p)).get(0));
            }
            printer.printRecord(record);
            numberOfLines ++;
            record.clear();
        }
        printer.close();csvStream.close();csvFileStream.close();
        logger.info("Done!");
        long fileSize = fs.getFileStatus(new Path(csvFile)).getLen();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_NUMBER_OF_LINES, numberOfLines);
        output.put(GraphOperators.ARG_FILE_SIZE, fileSize);
        loader.close();
        return output;
    }

    public static Map<String, Object> GopPageRankWithGraphComputerSerial(String graphName,
                                                                         String gremlinServerConfFile,
                                                                         String resultPropertyName,
                                                                         int returnTop) throws Exception {
        GremlinServerClient loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        long t0 = System.currentTimeMillis();
        Map<String, Double> topRanks = loader.pageRank(resultPropertyName, returnTop);
        long t1 = System.currentTimeMillis();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_ELAPSED_TIME, (t1 - t0));
        if (returnTop > 0)
            output.put(GraphOperators.ARG_TOP_VERTICES, topRanks);
        loader.close();
        return output;
    }

    public static Map<String, Object> GopPeerPressureWithGraphComputerSerial(String graphName,
                                                                         String gremlinServerConfFile,
                                                                         String resultPropertyName) throws Exception {
        GremlinServerClient loader = getLoaderFromConfFile(gremlinServerConfFile, graphName);
        long t0 = System.currentTimeMillis();
        long clusterCount = loader.peerPressure(resultPropertyName);
        long t1 = System.currentTimeMillis();
        Map<String, Object> output = new HashMap<>();
        output.put(GraphOperators.ARG_ELAPSED_TIME, (t1 - t0));
        output.put(GraphOperators.ARG_NUMBER_OF_CLUSTERS, clusterCount);
        loader.close();
        return output;
    }

    private static GremlinServerClient getLoaderFromConfFile(String gremlinConfFileOnHDFS, String graphName) throws IOException {
        File localGremlinServerConfFile = HDFSUtils.getHDFSFileToTmpLocal(gremlinConfFileOnHDFS);
        GremlinServerClient loader = new GremlinServerClient(localGremlinServerConfFile, graphName);
        HDFSUtils.deleteLocalTmpFile(localGremlinServerConfFile);
        return loader;
    }

    private static Vertex getOrCreateVertex(Graph g, String name) {
        return g.traversal().V().has("name", name).
                fold().
                coalesce(unfold(),
                        addV("v").property("name", name)).next();
    }

    private static Edge addEdge(Graph graph, Vertex src, Vertex dst,
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

    public static void GopCSVFileToGryoGraphSingleClient(String edgeCSVFilePath, String edgeSrcColumn,
                                                         String edgeDstColumn,
                                                         List<String> edgePropertyColumns,
                                                         boolean directed,
                                                         String outputFilePath) throws IOException {
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
        graph.io(GryoIo.build(GryoVersion.V1_0)).writer().create().writeGraph(outputStream, graph);
        outputStream.close();
        logger.info("Save the gryo file at: " + new Path(outputFilePath).toUri());
    }

    private static TinkerGraph loadGryoGraphToTinkerGraph(String inputGraphPath) throws IOException {
        ///// Input
        FileSystem fs = HDFSUtils.getFS(inputGraphPath);
        TinkerGraph graph = TinkerGraph.open();
        logger.info("Start to load graph...");
        try(final InputStream graphFileStream = fs.open(new Path(inputGraphPath))) {
            graph.io(GryoIo.build(GryoVersion.V1_0)).reader().create().readGraph(graphFileStream, graph);
        }
       System.out.println("Load gryo graph into memory done!");
        System.out.println("|V|=" + graph.traversal().V().count().next()
                + "\n|E|=" + graph.traversal().E().count().next());
        //System.err.println(graph.traversal().V().valueMap(true).next());
        //System.err.println(graph.traversal().E().valueMap(true).next());
        return graph;
    }

    public static void GopGryoGraphVertexToCSVFileSingleClient(String inputGraphPath,
                                                               List<String> properties,
                                                               String outputCSVFilePath) throws Exception {

        FileSystem fs = HDFSUtils.getFS(inputGraphPath);
        Graph graph = loadGryoGraphToTinkerGraph(inputGraphPath);
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

    public static void GopLabelPropagationGryoToGryoSingleClient(String inputGraphPath,
                                                                 String outputGraphPath,
                                                                 String resultPropertyName) throws IOException, ExecutionException, InterruptedException {
        logger.info("Load graph into memory.");
        TinkerGraph graph = loadGryoGraphToTinkerGraph(inputGraphPath);
        logger.info("Compute peerpressure.");
        ComputerResult result =
                graph.compute().program(PeerPressureVertexProgram.build()
                        .property(resultPropertyName)
                        .distributeVote(true).create(graph))
                        .submit().get();
        result.graph().vertices().forEachRemaining(vertex -> {
            graph.vertices(vertex.id()).next().property(resultPropertyName, vertex.value(resultPropertyName));
        });
        System.err.println(graph.traversal().V().valueMap(true).next());
        logger.info("Start to output.");
        FileSystem fs = HDFSUtils.getFS(outputGraphPath);
        try(OutputStream out = fs.create(new Path(outputGraphPath), true)) {
            graph.io(GryoIo.build(GryoVersion.V1_0)).writer().create().writeGraph(out, graph);
        }
        logger.info("Done!");
    }


}
