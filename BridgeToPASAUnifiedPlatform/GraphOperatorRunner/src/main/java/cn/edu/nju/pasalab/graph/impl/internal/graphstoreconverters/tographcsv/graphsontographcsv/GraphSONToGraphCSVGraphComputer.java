package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv;

import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH;

public class GraphSONToGraphCSVGraphComputer {
    public static void converter(Map<String, String> arguments) throws Exception {
    //public static void main(String[] args) throws Exception {
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

        /*String outputVertexCSVFilePath = "file:///home/lijunhong/graphxtosontest/hadoopcsvvertex";
        String outputEdgeCSVFilePath = "file:///home/lijunhong/graphxtosontest/hadoopcsvedge";
        String inputGraphFilePath = "file:///home/lijunhong/graphxtosontest/hadoophaha.txt";
        List<String> edgeProperties = ArgumentUtils.toList("weight");
        List<String> vertexProperties = ArgumentUtils.toList("test");*/
        /////////// Prepare vertex output
        Path outputVertexDirPath = new Path(outputVertexCSVFilePath);
        Path vertexSchemaFilePath = new Path(outputVertexDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_SCHEMA_FILE_NAME);
        Path vertexDataFilePath = new Path(outputVertexDirPath, cn.edu.nju.pasalab.graph.Constants.CSV_DATA_FILE_NAME);
        FileSystem fs = HDFSUtils.getFS(outputVertexCSVFilePath);
        fs.delete(new Path(outputVertexCSVFilePath), true);
        fs.mkdirs(new Path(outputVertexCSVFilePath));

        /////////// Prepare edge output
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

        String edgePropertyColumns = edgeSchema.toPropertiesString(false);


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

        String vertexPropertyColumns = vertexSchema.toPropertiesString(true);

        Configuration conf = new Configuration();
        conf.set("edgePropertyColumns", edgePropertyColumns);
        conf.set("vertexPropertyColumns", vertexPropertyColumns);
        Job job = new Job(conf, "Write a File");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SONToCSVmap.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(GraphSONInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputGraphFilePath));

        MultipleOutputs.addNamedOutput(job,"Edge",TextOutputFormat.class,NullWritable.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"Vertex",TextOutputFormat.class,NullWritable.class,Text.class);
        FileOutputFormat.setOutputPath(job, edgeDataFilePath);
        job.setJarByClass(GraphSONToGraphCSVGraphComputer.class);
        job.waitForCompletion(true);

    }
}

class SONToCSVmap extends Mapper<NullWritable, VertexWritable, NullWritable, Text> {
    private Text edgecsv = new Text();
    private Text vertexcsv = new Text();
    private List<String> edgePropertyColumns = new ArrayList<>();
    private List<String> vertexPropertyColumns = new ArrayList<>();
    private MultipleOutputs<NullWritable, Text> mos;
    public void setup(Context context) throws IOException{
        edgePropertyColumns = ArgumentUtils.toList(context.getConfiguration().get("edgePropertyColumns"));
        vertexPropertyColumns = ArgumentUtils.toList(context.getConfiguration().get("vertexPropertyColumns"));
        mos = new MultipleOutputs<NullWritable, Text>(context);
    }
    public void map(NullWritable key, VertexWritable value, Context context)
            throws IOException, InterruptedException {

        Vertex star = value.get();
        Iterator<Edge> edgeIter = star.edges(Direction.IN);

        String vertexProperties = star.id().toString();
        System.out.println(vertexPropertyColumns);
        for (int i = 0; i < vertexPropertyColumns.size(); i++) {
            vertexProperties += (","+star.property(vertexPropertyColumns.get(i)).value().toString());
        }
        vertexcsv.set(vertexProperties);
        mos.write("Vertex",NullWritable.get(),vertexcsv,"vertexcsv/");

        while (edgeIter.hasNext()){
            Edge edgeInfo = edgeIter.next();
            String edgeProperties = edgeInfo.outVertex().id().toString();
            for (int i = 0; i < edgePropertyColumns.size(); i++) {
                edgeProperties += (","+edgeInfo.property(edgePropertyColumns.get(i)).value().toString());
            }
            edgecsv.set(edgeInfo.inVertex().id().toString() + "," + edgeProperties);
            mos.write("Edge",NullWritable.get(), edgecsv,"edgecsv/");
        }
    }
    protected void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();
    }
}

