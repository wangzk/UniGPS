package cn.edu.nju.pasalab.graph.impl.graphx;

import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common;
import cn.edu.nju.pasalab.graph.impl.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.impl.util.CSVUtils;
import cn.edu.nju.pasalab.graph.impl.util.ConfUtils;
import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils;
import cn.edu.nju.pasalab.graphx.ToGraphx;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.*;

import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.VertexRDD;
import scala.Tuple3;
import scala.reflect.ClassTag;

import static cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_GRAPH;
import static cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_TMP_GRAPH_DIR_NAME;

public class GopLabelPropagation {

    public static void fromGraphSONToGraphSON(Map<String, String> arguments) throws Exception {

        //////////// Arguments
        String inputGraphFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        String graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);
        String outputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);

        ////////// Input graph
        org.apache.commons.configuration.Configuration inputGraphConf = new BaseConfiguration();
        inputGraphConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION,  inputGraphFilePath);
        inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath);
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputGraphPath);

        Common.ManageSparkContexts manageSparkContexts = new Common.ManageSparkContexts(graphComputerConfFile, "Gryo Vertex File to Graphx");
        SparkContext sc = manageSparkContexts.getSc();
        JavaSparkContext jsc = manageSparkContexts.getJsc();

        InputRDD graphRDDInput = new InputFormatRDD();
        JavaPairRDD<Object, VertexWritable> vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc);

/*        JavaRDD<Tuple2<Object,String>> vertex = vertexWritableJavaPairRDD.map(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            Long vid = Hashing.sha256().hashString(v.id().toString(), Charsets.UTF_8).asLong();
            Map<String,Object> graphxValueMap = new HashMap<>();
            graphxValueMap.put("originalID",v.id());
            graphxValueMap.putAll(g.traversal().V(v.id()).valueMap().next(1).get(0));

            Tuple2<Object,String> valmap = new Tuple2<>(vid,graphxValueMap.toString());
            return valmap;
        });

        JavaRDD<List> edgeValueMapRDD = vertexWritableJavaPairRDD.map(tuple2 -> {
            StarGraph.StarVertex v = tuple2._2.get();
            StarGraph g = StarGraph.of(v);
            Long vid = Hashing.sha256().hashString(v.id().toString(), Charsets.UTF_8).asLong();
            List pathlist = g.traversal().V(v.id()).out().path().by(T.id).toList();
            return pathlist;
        });

        JavaRDD edgeIter = edgeValueMapRDD.flatMap(list -> {
             return list.iterator();
        });


        JavaRDD edge = edgeIter.map(pair ->{
            String pairname = pair.toString();
            Gson gson = new Gson();
            ArrayList<String> array = new ArrayList<String>();
            array = gson.fromJson(pairname, array.getClass());
            Long md1 = Hashing.sha256().hashString(array.get(0), Charsets.UTF_8).asLong();
            Long md2 = Hashing.sha256().hashString(array.get(1), Charsets.UTF_8).asLong();
            return new Edge<String>(md1,md2,"1");
        });



        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        Graph graph = Graph.apply(vertex.rdd(),edge.rdd(),"", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);*/



        ToGraphx gx = new ToGraphx();
        gx.initGraphxFromJavapair(vertexWritableJavaPairRDD);



        ////////// Output graph
        FileSystem fs = HDFSUtils.getFS(outputGraphPath);
        fs.delete(new Path(outputGraphPath), true);


        manageSparkContexts.stop();
/*        fs.rename(new Path(outputGraphPath, GREMLIN_TMP_GRAPH_DIR_NAME), new Path(outputGraphPath + "~"));
        fs.delete(new Path(outputGraphPath), true);
        fs.rename(new Path(outputGraphPath + "~"), new Path(outputGraphPath));*/
    }


}
