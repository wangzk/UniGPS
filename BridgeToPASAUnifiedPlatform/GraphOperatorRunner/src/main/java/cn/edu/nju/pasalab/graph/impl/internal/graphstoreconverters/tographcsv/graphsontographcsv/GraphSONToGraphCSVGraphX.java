package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH;

public class GraphSONToGraphCSVGraphX {

    public static void converter(Map<String, String> arguments) throws Exception {
        //////////// Arguments
        // For GraphSON file, the conf path is the graph file path
        String inputGraphFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        List<String> vertexProperties =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_VERTEX_PROPERTY_NAMES));
        List<String> edgeProperties =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_PROPERTY_NAMES));
        String outputVertexCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH);
        String outputEdgeCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_EDGE_CSV_FILE_PATH);
        String graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);

        /////////// Prepare vertex output
        Path outputVertexDirPath = new Path(outputVertexCSVFilePath);
        Path vertexSchemaFilePath = new Path(outputVertexDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_SCHEMA_FILE_NAME);
        Path vertexDataFilePath = new Path(outputVertexDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_DATA_FILE_NAME);
        FileSystem fs = HDFSUtils.getFS(outputVertexCSVFilePath);
        fs.delete(new Path(outputVertexCSVFilePath), true);
        fs.mkdirs(new Path(outputVertexCSVFilePath));

        /////////// Prepare vertex output
        Path outputEdgeDirPath = new Path(outputEdgeCSVFilePath);
        Path edgeSchemaFilePath = new Path(outputEdgeDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_SCHEMA_FILE_NAME);
        Path edgeDataFilePath = new Path(outputEdgeDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_DATA_FILE_NAME);
        fs.delete(new Path(outputEdgeCSVFilePath), true);
        fs.mkdirs(new Path(outputEdgeCSVFilePath));

        ////////// Input graph
        org.apache.commons.configuration.Configuration inputGraphConf = new BaseConfiguration();
        inputGraphConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION,  inputGraphFilePath);
        inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath);
        HadoopGraph inputGraph = HadoopGraph.open(inputGraphConf);

        ////////// Output edge schema
        CSVUtils.CSVSchema edgeSchema = null;

        try(final OutputStream out = fs.create(edgeSchemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Edge sampleEdge = inputGraph.edges().next();
            edgeSchema = new CSVUtils.CSVSchema(edgeProperties, sampleEdge);
            writer.print(edgeSchema.toSchemaDescription());
            writer.close();
            System.out.println(edgeSchema.toSchemaDescription());
        }

        final CSVUtils.CSVSchema edgeSchema_b = edgeSchema;


        ////////// Output vertex schema
        CSVUtils.CSVSchema vertexSchema = null;
        if (!vertexProperties.contains("name")) {
            vertexProperties.add(0, "name");
        }
        try(final OutputStream out = fs.create(vertexSchemaFilePath, true)) {
            PrintWriter writer = new PrintWriter(out);
            Vertex sampleVertex = inputGraph.vertices().next();
            vertexSchema = new CSVUtils.CSVSchema(vertexProperties, sampleVertex);
            writer.print(vertexSchema.toSchemaDescription());
            writer.close();
            System.out.println(vertexSchema.toSchemaDescription());
        }

        final CSVUtils.CSVSchema vertexSchema_b = vertexSchema;

        ////////// Output vertex data
        CommonGraphComputer.ManageSparkContexts manageSparkContexts = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to CSV File");
        SparkContext sc = manageSparkContexts.getSc();
        JavaSparkContext jsc = manageSparkContexts.getJsc();

        InputRDD graphRDDInput = new InputFormatRDD();
        JavaPairRDD<Object, VertexWritable> vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc);

        JavaRDD<String> vertexCsvLineRDD = vertexWritableJavaPairRDD.map(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            StringBuilder csvLine = new StringBuilder();
            csvLine.append("," + Long.parseLong(v.id().toString(), 10));
            for(String pName: vertexProperties) {
                Object propertyValue = g.traversal().V(v.id()).values(pName).next();
                CSVUtils.CSVSchema.PropertyType type = vertexSchema_b.getColumnType().get(pName);
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
        for(String line:vertexCsvLineRDD.collect()){
            System.out.println("* "+line);
        }
        vertexCsvLineRDD.saveAsTextFile(vertexDataFilePath.toString());

        ////////// Output edge data
        JavaRDD<Edge> edgelistRDD = vertexWritableJavaPairRDD.flatMap(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            return g.traversal().V(v.id()).outE().toList().iterator();
        });

        JavaRDD<String> edgeCsvLineRDD = edgelistRDD.map(edge -> {
            StringBuilder csvLine = new StringBuilder();
            csvLine.append("," + Long.parseLong(edge.inVertex().id().toString(), 10));
            csvLine.append("," + Long.parseLong(edge.outVertex().id().toString(), 10));
            for(String pName: edgeProperties) {
                Object propertyValue = edge.values(pName).next();
                CSVUtils.CSVSchema.PropertyType type = edgeSchema_b.getColumnType().get(pName);
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

        for(String line:edgeCsvLineRDD.collect()){
            System.out.println("* "+line);
        }
        edgeCsvLineRDD.saveAsTextFile(edgeDataFilePath.toString());
        manageSparkContexts.stop();
    }
}
