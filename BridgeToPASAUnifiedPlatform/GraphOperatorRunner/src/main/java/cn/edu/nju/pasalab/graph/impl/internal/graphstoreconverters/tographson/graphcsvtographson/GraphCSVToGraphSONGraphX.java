package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
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

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial.logger;

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
        if (!edgePropertyColumns.contains(edgeSrcColumn)) {
            edgePropertyColumns.add(0, edgeSrcColumn);
        }
        if (!edgePropertyColumns.contains(edgeDstColumn)) {
            edgePropertyColumns.add(1, edgeDstColumn);
        }

        ///////// Init spark context
        JavaSparkContext jsc = null;
        if (graphComputerConfFile != null){
            CommonGraphComputer.ManageSparkContexts msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "CSV File To GraphSON File");
            jsc = msc.getJsc();
        } else {
            SparkConf sparkConf = new SparkConf(true);
            sparkConf.setAppName("GraphCSV to GraphSON under Serial mode").setMaster("local");
            jsc = new JavaSparkContext(sparkConf);
        }


        ///////// Parse Edge CSV file
        JavaPairRDD<String, Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>> edgePairRDD =
                new CSVUtils.EdgeCSVParser(edgeCSVFilePath, jsc, edgePropertyColumns, edgeSrcColumn
                ,edgeDstColumn,directed).get();

        JavaPairRDD<String, Iterable<Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>>> vertexPairRDD =
                edgePairRDD.groupByKey();

        ///////// Parse Vertex CSV file
        JavaRDD<Vertex> vertexRDD = null;
        if (vertexCSVFilePath != null){

            JavaPairRDD<String, Map<String, Object>> vertexAttrPairRDD = new CSVUtils.VertexCSVParser(vertexCSVFilePath,
                    jsc, vertexPropertyColumns, vertexNameColumn, directed).get();

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
        HDFSUtils.getFS(outputFilePath).rename(new Path(tmpOutputPath, CommonGraphComputer.GREMLIN_TMP_GRAPH_DIR_NAME), new Path(outputFilePath));
        HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true);
        logger.info("Save the GraphSON file at: " + new Path(outputFilePath).toUri());
    }
}
