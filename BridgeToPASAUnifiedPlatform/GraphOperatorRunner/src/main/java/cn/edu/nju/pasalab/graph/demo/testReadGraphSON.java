package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.io.OutputStream;
import java.io.PrintWriter;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH;

public class testReadGraphSON {
    public static void main(String[] args) {

        String inputGraphFilePath = "/home/lijunhong/graphxtosontest/tmp_181212204314";
        org.apache.commons.configuration.Configuration inputGraphConf = new BaseConfiguration();
        inputGraphConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION,  inputGraphFilePath);
        inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath);

        // HadoopGraph EDGE MUST APPEAR TWICE!!!!!!!!! SEE THE graphdb to graphson
        HadoopGraph inputGraph = HadoopGraph.open(inputGraphConf);

        Edge sampleEdge = inputGraph.edges().next();
        System.out.println(sampleEdge.id());
    }
}
