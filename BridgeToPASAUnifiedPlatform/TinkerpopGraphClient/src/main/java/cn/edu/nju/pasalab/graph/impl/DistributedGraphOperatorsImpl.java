package cn.edu.nju.pasalab.graph.impl;

import cn.edu.nju.pasalab.graph.GraphOperators;
import cn.edu.nju.pasalab.graph.impl.util.CSVUtils;
import cn.edu.nju.pasalab.graph.impl.util.ConfUtils;
import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DistributedGraphOperatorsImpl {

    public static final String DEFAULT_VERTEX_LABEL = "SimpleV";
    public static final String DEFAULT_EDGE_LABEL = "SimpleE";
    public static final String GREMLIN_GRAPH = "gremlin.graph";
    public static final String GREMLIN_TMP_GRAPH_DIR_NAME = "~g";

    private static class ManageSparkContexts {

        public SparkContext getSc() {
            return sc;
        }

        public JavaSparkContext getJsc() {
            return jsc;
        }

        SparkContext sc;
        JavaSparkContext jsc;

        public ManageSparkContexts(String graphComputerConfFile, String appName) throws IOException, ConfigurationException {

            Configuration graphComputerConf = ConfUtils.loadConfFromHDFS(graphComputerConfFile);
            SparkConf sparkConf = new SparkConf(true);
            sparkConf.setAppName(appName);
            ConfUtils.loadUserConfToSparkConf(sparkConf, graphComputerConf);
            this.sc = new SparkContext(sparkConf);
            this.jsc = JavaSparkContext.fromSparkContext(sc);
        }

        public void stop() throws Exception {
            jsc.close();
            sc.stop();
        }

    }

    public enum VertexDirection {SRC, DST};

    private static class EdgeTupleIterToVertexFunction implements
            Function<Tuple2<String,Iterable<Tuple3<String, VertexDirection, Map<String, Object>>>>,
                    Vertex>
    {

        private class ProcessAdj {
            String centerVertexName;
            Iterable<Tuple3<String, VertexDirection, Map<String, Object>>> adjs;

            public ProcessAdj(String centerVertexName, Iterable<Tuple3<String, VertexDirection, Map<String, Object>>> adjs) {
                this.centerVertexName = centerVertexName;
                this.adjs = adjs;
            }

            private Vertex getOrCreate(String name) {
                if (cache.containsKey(name)) {
                    return cache.get(name);
                } else {
                    Vertex v = graph.addVertex(T.id, name, T.label, DEFAULT_VERTEX_LABEL, "name", name);
                    cache.put(name, v);
                    return v;
                }
            }

            private Edge addProperties(Edge e, Map<String, Object> properties) {
                properties.forEach((key, value) -> {
                    e.property(key.toString(), value);
                });
                return e;
            }

            StarGraph graph = StarGraph.open();
            HashMap<String, Vertex> cache = new HashMap<>();

            public Vertex constructVertex() {
                // Build center vertex
                Vertex centerVertex = getOrCreate(centerVertexName);
                // Add adjacency edges
                adjs.forEach(adjVertexTuple -> {
                    String anotherVertexName = adjVertexTuple._1();
                    VertexDirection direction = adjVertexTuple._2();
                    Map<String, Object> properties = adjVertexTuple._3();
                    Edge edge = null;
                    if (direction == VertexDirection.SRC) {
                        Vertex srcV = getOrCreate(anotherVertexName);
                        Vertex dstV = centerVertex;
                        edge = srcV.addEdge(DEFAULT_EDGE_LABEL, dstV);
                    } else if (direction == VertexDirection.DST) {
                        Vertex srcV = centerVertex;
                        Vertex dstV = getOrCreate(anotherVertexName);
                        edge = srcV.addEdge(DEFAULT_EDGE_LABEL, dstV);
                    }
                    if (edge != null && properties.size() > 0) {
                        addProperties(edge, properties);
                    }
                });
                // Return the center Vertex
                return graph.getStarVertex();
            }
        } // end of class

        @Override
        public Vertex call(Tuple2<String, Iterable<Tuple3<String, VertexDirection, Map<String, Object>>>> edgeTuples) throws Exception {
            String centerVertexName = edgeTuples._1();
            ProcessAdj processAdj = new ProcessAdj(centerVertexName, edgeTuples._2());
            Vertex centerVertex = processAdj.constructVertex();
            return centerVertex;
        }
    }

    public static void GopCSVFileToGryoGraphSpark(Map<String, Object> arguments) throws Exception {
        ////////// Arguments
        String edgeCSVFilePath = (String)arguments.get(GraphOperators.ARG_EDGE_CSV_FILE_PATH);
        String edgeSrcColumn = (String)arguments.get(GraphOperators.ARG_EDGE_SRC_COLUMN);
        String edgeDstColumn = (String)arguments.get(GraphOperators.ARG_EDGE_DST_COLUMN);
        Boolean directed = (Boolean)arguments.get(GraphOperators.ARG_DIRECTED);
        List<String> edgePropertyColumns = (List<String>)arguments.get(GraphOperators.ARG_EDGE_PROPERTY_COLUMNS);
        // For Gryo graph, the conf file path is also the output file path.
        String outputFilePath = (String)arguments.get(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE);
        String graphComputerConfFile = (String)arguments.get(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE);

        ///////// Input file
        Path edgeCSVFileHDFSPath = new Path(edgeCSVFilePath);
        Path dataFilePath = new Path(edgeCSVFileHDFSPath, "data.csv");
        Path schemaFilePath = new Path(edgeCSVFileHDFSPath, "schema");
        if (!edgePropertyColumns.contains(edgeSrcColumn)) {
            edgePropertyColumns.add(0, edgeSrcColumn);
        }
        if (!edgePropertyColumns.contains(edgeDstColumn)) {
            edgePropertyColumns.add(1, edgeDstColumn);
        }

        ///////// Init spark context
        ManageSparkContexts msc = new ManageSparkContexts(graphComputerConfFile, "CSV File To Gryo File");
        SparkContext sc = msc.getSc();
        JavaSparkContext jsc = msc.getJsc();

        ///////// Parse CSV file
        CSVUtils.CSVSchema csvSchema = new CSVUtils.CSVSchema(schemaFilePath);
        JavaRDD<Map<String,Object>> csvRDD = jsc.textFile(dataFilePath.toString()).map(csvLine -> {
           Map<String, Object> columns = csvSchema.parseCSVLine(csvLine);
           Map<String, Object> properties = new HashMap<>();
           edgePropertyColumns.forEach(property -> properties.put(property, columns.get(property)));
           return properties;
        });
        JavaRDD<Tuple3<String, String, Map<String, Object>>> edgeRDD = csvRDD.map(properties -> {
           return new Tuple3<>(properties.get(edgeSrcColumn).toString(),
                   properties.get(edgeDstColumn).toString(),
                   properties);
        });
        if (!directed) {
            edgeRDD = edgeRDD.union(csvRDD.map(properties -> {
                return new Tuple3<>(properties.get(edgeDstColumn).toString(),
                        properties.get(edgeSrcColumn).toString(),
                        properties);
            }));
        }
        ///////// Get groupped RDD && Construct vertex RDD
        JavaPairRDD<String, Tuple3<String, VertexDirection, Map<String, Object>>> edgePairRDD =
        edgeRDD.flatMapToPair((PairFlatMapFunction<Tuple3<String, String, Map<String, Object>>, String, Tuple3<String, VertexDirection, Map<String, Object>>>) edge -> {
            ArrayList<Tuple2<String, Tuple3<String, VertexDirection, Map<String, Object>>>> tuples = new ArrayList<>();
            tuples.add(new Tuple2<>(edge._1(), new Tuple3<>(edge._2(), VertexDirection.DST, edge._3())));
            tuples.add(new Tuple2<>(edge._2(), new Tuple3<>(edge._1(), VertexDirection.SRC, edge._3())));
            return tuples.iterator();
        });
        JavaRDD<Vertex> vertexRDD = edgePairRDD.groupByKey().map(new EdgeTupleIterToVertexFunction());

        ///////// Output the VertexRDD
        org.apache.commons.configuration.Configuration outputConf = new BaseConfiguration();
        String tmpOutputPath = outputFilePath + "~";
        outputConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        outputConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GraphSONOutputFormat.class.getName());
        outputConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,  tmpOutputPath);
        HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true);
        HDFSUtils.getFS(outputFilePath).delete(new Path(outputFilePath), true);
        HDFSUtils.getFS(outputFilePath).deleteOnExit(new Path(tmpOutputPath));

        JavaPairRDD<Object, VertexWritable> graphRDD = vertexRDD.mapToPair(
                (PairFunction<Vertex, Object, VertexWritable>)  vertex -> {
            return new Tuple2<>(NullWritable.get(), new VertexWritable(vertex));
        });
        OutputFormatRDD formatRDD = new OutputFormatRDD();
        formatRDD.writeGraphRDD(outputConf, graphRDD);
        jsc.close();
        sc.stop();
        HDFSUtils.getFS(outputFilePath).rename(new Path(tmpOutputPath, GREMLIN_TMP_GRAPH_DIR_NAME), new Path(outputFilePath));
        HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true);
    }

    public static void GopGryoGraphVertexToCSVFileSpark(Map<String, Object> arguments) throws Exception {
        //////////// Arguments
        // For Gryo graph file, the conf path is the graph file path
        String inputGraphFilePath = (String)arguments.get(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE);
        List<String> properties = (List<String>)arguments.get(GraphOperators.ARG_PROPERTIES);
        String outputCSVFilePath = (String)arguments.get(GraphOperators.ARG_OUTPUT_VERTEX_CSV_FILE_PATH);
        String graphComputerConfFile = (String)arguments.get(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE);

        /////////// Prepare output
        Path outputDirPath = new Path(outputCSVFilePath);
        Path schemaFilePath = new Path(outputDirPath, "schema");
        Path dataFilePath = new Path(outputDirPath, "data.csv");
        FileSystem fs = HDFSUtils.getFS(outputCSVFilePath);
        fs.delete(new Path(outputCSVFilePath), true);
        fs.mkdirs(new Path(outputCSVFilePath));
        ////////// Input graph
        org.apache.commons.configuration.Configuration inputGraphConf = new BaseConfiguration();
        inputGraphConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION,  inputGraphFilePath);
        inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath);
        HadoopGraph inputGraph = HadoopGraph.open(inputGraphConf);

        ////////// Output schema
        CSVUtils.CSVSchema schema = null;
        if (!properties.contains("name")) {
            properties.add(0, "name");
        }
        try(final OutputStream out = fs.create(schemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Vertex sampleVertex = inputGraph.vertices().next();
            schema = new CSVUtils.CSVSchema(properties, sampleVertex);
            writer.print(schema.toSchemaDescription());
            writer.close();
            System.out.println(schema.toSchemaDescription());
        }

        final CSVUtils.CSVSchema schema_b = schema;

        ////////// Output data
        ManageSparkContexts manageSparkContexts = new ManageSparkContexts(graphComputerConfFile, "Gryo Vertex File to CSV File");
        SparkContext sc = manageSparkContexts.getSc();
        JavaSparkContext jsc = manageSparkContexts.getJsc();

        InputRDD graphRDDInput = new InputFormatRDD();
        JavaPairRDD<Object, VertexWritable> vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc);
        JavaRDD<String> csvLineRDD = vertexWritableJavaPairRDD.map(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            StringBuilder csvLine = new StringBuilder();
            for(String pName: properties) {
                Object propertyValue = g.traversal().V(v.id()).values(pName).next();
                CSVUtils.CSVSchema.PropertyType type = schema_b.getColumnType().get(pName);
                switch (type) {
                    case DOUBLE:
                        csvLine.append("," + propertyValue);
                        break;
                    case INT:
                        csvLine.append("," + propertyValue);
                        break;
                    case STRING:
                        csvLine.append(",\"" + propertyValue + "\"");
                        break;
                }
            }
            return csvLine.substring(1);
        });

        csvLineRDD.saveAsTextFile(dataFilePath.toString());
        manageSparkContexts.stop();
    }

    public static void GopLabelPropagationGryoToGryo(Map<String, Object> arguments) throws Exception {
        String inputGraphPath = (String)arguments.get(GraphOperators.ARG_INPUT_GRAPH_CONF_FILE);
        String outputGraphPath = (String)arguments.get(GraphOperators.ARG_OUTPUT_GRAPH_CONF_FILE);
        String resultPropertyName = (String)arguments.get(GraphOperators.ARG_RESULT_PROPERTY_NAME);
        String graphComputerConfPath = (String)arguments.get(GraphOperators.ARG_GRAPH_COMPUTER_CONF_FILE);
        FileSystem fs = HDFSUtils.getFS(outputGraphPath);
        fs.delete(new Path(outputGraphPath), true);
        Configuration graphComputerConf = ConfUtils.loadConfFromHDFS(graphComputerConfPath);
        graphComputerConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphPath);
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputGraphPath);
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GraphSONOutputFormat.class.getName());
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, "true");
        HadoopGraph inputGraph = HadoopGraph.open(graphComputerConf);
        PeerPressureVertexProgram ppvp = PeerPressureVertexProgram.build()
                .maxIterations(10)
                .distributeVote(true).property(resultPropertyName).create(inputGraph);
        //ComputerResult result = inputGraph.compute(SparkGraphComputer.class).program(ppvp).submit().get();
        inputGraph.traversal().withComputer(SparkGraphComputer.class).V().peerPressure().by(resultPropertyName).iterate();
        //System.out.println(result.memory());
        inputGraph.close();
        fs.rename(new Path(outputGraphPath, GREMLIN_TMP_GRAPH_DIR_NAME), new Path(outputGraphPath + "~"));
        fs.delete(new Path(outputGraphPath), true);
        fs.rename(new Path(outputGraphPath + "~"), new Path(outputGraphPath));
    }


}
