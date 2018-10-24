package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphCSVToGraphSONGraphX {
    public static void converter(Map<String, String> arguments) throws Exception {
        ////////// Arguments
        String vertexCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_VERTEX_CSV_FILE_PATH);
        String edgeCSVFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_EDGE_CSV_FILE_PATH);
        String edgeSrcColumn = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_SRC_COLUMN);
        String edgeDstColumn = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_DST_COLUMN);
        String vertexNameColumn = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_VERTEX_NAME_COLUMN);
        Boolean directed = Boolean.valueOf(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_DIRECTED));
        List<String> edgePropertyColumns =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_EDGE_PROPERTY_COLUMNS));
        List<String> vertexPropertyColumns =
                ArgumentUtils.toList(arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_VERTEX_PROPERTY_COLUMNS));
        // For GraphSON, the conf file path is also the output file path.
        String outputFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
        // For spark graph computer, the graph computer file is the run mode conf file
        String graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);

        ///////// Input Edge file
        Path edgeCSVFileHDFSPath = new Path(edgeCSVFilePath);
        Path edgeDataFilePath = new Path(edgeCSVFileHDFSPath, cn.edu.nju.pasalab.graph.Constants.CSV_DATA_FILE_NAME);
        Path edgeSchemaFilePath = new Path(edgeCSVFileHDFSPath, cn.edu.nju.pasalab.graph.Constants.CSV_SCHEMA_FILE_NAME);
        if (!edgePropertyColumns.contains(edgeSrcColumn)) {
            edgePropertyColumns.add(0, edgeSrcColumn);
        }
        if (!edgePropertyColumns.contains(edgeDstColumn)) {
            edgePropertyColumns.add(1, edgeDstColumn);
        }


        ///////// Init spark context
        CommonGraphComputer.ManageSparkContexts msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "CSV File To GraphSON File");
        SparkContext sc = msc.getSc();
        JavaSparkContext jsc = msc.getJsc();

        ///////// Parse Edge CSV file
        CSVUtils.CSVSchema edgeCsvSchema = new CSVUtils.CSVSchema(edgeSchemaFilePath);
        JavaRDD<Map<String,Object>> edgeCsvRDD = jsc.textFile(edgeDataFilePath.toString()).map(csvLine -> {
            Map<String, Object> columns = edgeCsvSchema.parseCSVLine(csvLine);
            Map<String, Object> properties = new HashMap<>();
            edgePropertyColumns.forEach(property -> properties.put(property, columns.get(property)));
            return properties;
        });

        JavaRDD<Tuple3<String, String, Map<String, Object>>> edgeRDD = edgeCsvRDD.map(properties -> {
            return new Tuple3<>(properties.get(edgeSrcColumn).toString(),
                    properties.get(edgeDstColumn).toString(),
                    properties);
        });
        if (!directed) {
            edgeRDD = edgeRDD.union(edgeCsvRDD.map(properties -> {
                return new Tuple3<>(properties.get(edgeDstColumn).toString(),
                        properties.get(edgeSrcColumn).toString(),
                        properties);
            }));
        }
        ///////// Get groupped RDD && Construct vertex RDD
        JavaPairRDD<String, Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>> edgePairRDD =
                edgeRDD.flatMapToPair((PairFlatMapFunction<Tuple3<String, String, Map<String, Object>>, String, Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>>) edge -> {
                    ArrayList<Tuple2<String, Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>>> tuples = new ArrayList<>();
                    tuples.add(new Tuple2<>(edge._1(), new Tuple3<>(edge._2(), CommonGraphComputer.VertexDirection.DST, edge._3())));
                    tuples.add(new Tuple2<>(edge._2(), new Tuple3<>(edge._1(), CommonGraphComputer.VertexDirection.SRC, edge._3())));
                    return tuples.iterator();
                });
        //JavaRDD<Vertex> vertexRDD = edgePairRDD.groupByKey().map(new CommonGraphComputer.EdgeTuplesToVertex());


        JavaPairRDD<String, Iterable<Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>>> vertexPairRDD =
                edgePairRDD.groupByKey();

        JavaRDD<Vertex> vertexRDD = null;

        if (vertexCSVFilePath != null){
            ///////// Input Vertex file
            Path vertexCSVFileHDFSPath = new Path(vertexCSVFilePath);
            Path vertexDataFilePath = new Path(vertexCSVFileHDFSPath, cn.edu.nju.pasalab.graph.Constants.CSV_DATA_FILE_NAME);
            Path vertexSchemaFilePath = new Path(vertexCSVFileHDFSPath, cn.edu.nju.pasalab.graph.Constants.CSV_SCHEMA_FILE_NAME);
            if (!vertexPropertyColumns.contains(vertexNameColumn)) {
                vertexPropertyColumns.add(0, vertexNameColumn);
            }
            ///////// Parse Vertex CSV file
            CSVUtils.CSVSchema vertexCsvSchema = new CSVUtils.CSVSchema(vertexSchemaFilePath);
            JavaRDD<Map<String,Object>> vertexCsvRDD = jsc.textFile(vertexDataFilePath.toString()).map(csvLine -> {
                Map<String, Object> columns = vertexCsvSchema.parseCSVLine(csvLine);
                Map<String, Object> properties = new HashMap<>();
                vertexPropertyColumns.forEach(property -> properties.put(property, columns.get(property)));
                return properties;
            });
            JavaRDD<Tuple2<String, Map<String, Object>>> vertexAttrRDD = vertexCsvRDD.map(properties -> {
                return new Tuple2<>(properties.get(vertexNameColumn).toString(),
                        properties);
            });
            JavaPairRDD<String, Map<String, Object>> vertexAttrPairRDD = vertexAttrRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, Map<String, Object>>) edge -> {
                ArrayList<Tuple2<String, Map<String, Object>>> tuples = new ArrayList<>();
                tuples.add(new Tuple2<>(edge._1(), edge._2));
                return tuples.iterator();});

            vertexRDD = vertexPairRDD.leftOuterJoin(vertexAttrPairRDD).map(new CommonGraphComputer.EdgeTuplesToVertexWithVProperties());
        } else {
            vertexRDD = vertexPairRDD.map(new CommonGraphComputer.EdgeTuplesToVertex());
        }



        ///////// Output the VertexRDD
        org.apache.commons.configuration.Configuration outputConf = new BaseConfiguration();
        String tmpOutputPath = outputFilePath + "~";
        outputConf.setProperty(CommonGraphComputer.GREMLIN_GRAPH, HadoopGraph.class.getName());
        outputConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GraphSONOutputFormat.class.getName());
        outputConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,  tmpOutputPath);
        HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true);
        HDFSUtils.getFS(outputFilePath).delete(new Path(outputFilePath), true);
        HDFSUtils.getFS(outputFilePath).deleteOnExit(new Path(tmpOutputPath));

        JavaPairRDD<Object, VertexWritable> graphRDD = vertexRDD.mapToPair(
                (PairFunction<Vertex, Object, VertexWritable>) vertex -> {
                    return new Tuple2<>(NullWritable.get(), new VertexWritable(vertex));
                });
        OutputFormatRDD formatRDD = new OutputFormatRDD();
        formatRDD.writeGraphRDD(outputConf, graphRDD);
        jsc.close();
        sc.stop();
        HDFSUtils.getFS(outputFilePath).rename(new Path(tmpOutputPath, CommonGraphComputer.GREMLIN_TMP_GRAPH_DIR_NAME), new Path(outputFilePath));
        HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true);
    }
}
