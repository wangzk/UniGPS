package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.util.ArgumentUtils;
import cn.edu.nju.pasalab.graph.util.CSVUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;

public class GraphCSVToGraphSONGraphComputer {

    public static void converter(Map<String, String> arguments) throws Exception {
    //public static void main(String[] args) throws Exception {


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

         //TEST
        /*
        String outputGraphFilePath = "file:///home/lijunhong/graphxtosontest/hadoopcsvtest";
        String inputGraphFilePath = "file:///home/lijunhong/graphxtosontest/hahahaha.txt";
        String edgeCSVFilePath = "/home/lijunhong/graphxtosontest/test.csv";
        String vertexCSVFilePath = "/home/lijunhong/graphxtosontest/vertex.csv";
        String outputFilePath ="/home/lijunhong/graphxtosontest/hadoophaha.txt";
        String edgeSrcColumn = "p1";
        String edgeDstColumn = "p2";
        List<String> edgePropertyColumns = ArgumentUtils.toList("weight");
        List<String> vertexPropertyColumns = ArgumentUtils.toList("test");*/

        ///////// Input Edge file
        if (!edgePropertyColumns.contains(edgeSrcColumn)) {
            edgePropertyColumns.add(0, edgeSrcColumn);
        }
        if (!edgePropertyColumns.contains(edgeDstColumn)) {
            edgePropertyColumns.add(1, edgeDstColumn);
        }

        if (!vertexPropertyColumns.contains("name")) {
            vertexPropertyColumns.add(0, "name");
        }

        Configuration conf = new Configuration();
        conf.set("edgePropertyColumns", StringUtils.join(edgePropertyColumns,","));
        conf.set("vertexPropertyColumns", StringUtils.join(vertexPropertyColumns,","));
        conf.set("edgeCSVFilePath",edgeCSVFilePath);
        conf.set("vertexCSVFilePath",vertexCSVFilePath);
        Job job = new Job(conf, "Write a File");

        FileSystem fs = FileSystem.get(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (fs.exists(new Path(outputFilePath)))
            fs.delete(new Path(outputFilePath), true);
        job.setMapperClass(EdgeMapper.class);
        job.setReducerClass(CSVToSONreduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(GraphSONOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path("file://" + edgeCSVFilePath + "/data.csv"),
                TextInputFormat.class,EdgeMapper.class);
        MultipleInputs.addInputPath(job, new Path("file://" + vertexCSVFilePath + "/data.csv"),
                TextInputFormat.class, VertexMapper.class);


        FileOutputFormat.setOutputPath(job, new Path("file://" + outputFilePath));
        job.setJarByClass(GraphCSVToGraphSONGraphComputer.class);
        job.waitForCompletion(true);

    }
}

class EdgeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String edgePath = "";
    private CSVUtils.CSVSchema edgeCsvSchema;
    private List<String> propertyColumns = new ArrayList<>();

    public void setup(Context context) throws IOException{
        edgePath = "file://" + context.getConfiguration().get("edgeCSVFilePath");
        Path schemaFilePath = new Path(edgePath, "schema");
        edgeCsvSchema = new CSVUtils.CSVSchema(schemaFilePath);
        propertyColumns = ArgumentUtils.toList(context.getConfiguration().get("edgePropertyColumns"));
    }

    private Text src = new Text();
    private Text neighbour = new Text();
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Map<String, Object> columns = edgeCsvSchema.parseCSVLine(value.toString());
        List<Object> properties = new ArrayList<>();
        propertyColumns.forEach(property -> properties.add(columns.get(property)));
        String edgeSrc = properties.toArray()[0].toString();
        String edgeInfo = StringUtils.join(properties.toArray(),",");
        src.set(edgeSrc);
        neighbour.set("E:"+edgeInfo);
        context.write(src, neighbour);
    }
}

class VertexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String vertexPath = "";
    private CSVUtils.CSVSchema vertexCsvSchema;
    private List<String> propertyColumns = new ArrayList<>();

    public void setup(Context context) throws IOException{
        vertexPath = "file://" + context.getConfiguration().get("vertexCSVFilePath");
        Path schemaFilePath = new Path(vertexPath, "schema");
        vertexCsvSchema = new CSVUtils.CSVSchema(schemaFilePath);
        propertyColumns = ArgumentUtils.toList(context.getConfiguration().get("vertexPropertyColumns"));
    }

    private Text src = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Map<String, Object> columns = vertexCsvSchema.parseCSVLine(value.toString());
        List<Object> properties = new ArrayList<>();

        propertyColumns.forEach(property -> properties.add(columns.get(property)));
        String vertexID = properties.toArray()[0].toString();
        String vertexInfo = StringUtils.join(properties.toArray(),",");
        src.set(vertexID);
        context.write(src, new Text("V:"+vertexInfo));
    }
}

class CSVToSONreduce extends Reducer<Text, Text, NullWritable, VertexWritable> {

    private List<Text> vertexValues = new ArrayList<>();
    private List<Text> edgeValues = new ArrayList<>();

    private List<Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>> adjsList =new ArrayList<>();

    public <T> Iterable<T> iteratorToIterable(Iterator<T> iterator) {
        return () -> iterator;
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> edgePropertyColumns = ArgumentUtils.toList(context.getConfiguration().get("edgePropertyColumns"));
        String edgePath = "file://" + context.getConfiguration().get("edgeCSVFilePath");
        Path edgeSchemaFilePath = new Path(edgePath, "schema");
        CSVUtils.CSVSchema edgeCsvSchema = new CSVUtils.CSVSchema(edgeSchemaFilePath);
        adjsList.clear();
        edgeValues.clear();
        for(Text data : values)
        {
            if(data.toString().startsWith("E"))
            {
                edgeValues.add(new Text(data.toString().split(":")[1]));
            }
            else {
                vertexValues.add(new Text(data.toString().split(":")[1]));
            }
        }

        for (Text val : edgeValues) {
            List<String> edgeInfo = ArgumentUtils.toList(val.toString());
            Map<String, Object> edgeProperties = new HashMap<>();
            for (int i = 0; i < edgePropertyColumns.size(); i++) {
                CSVUtils.CSVSchema.PropertyType type = edgeCsvSchema.getColumnType().get(edgePropertyColumns.get(i));
                if (type.equals(CSVUtils.CSVSchema.PropertyType.STRING)) {
                    edgeProperties.put(edgePropertyColumns.get(i),edgeInfo.get(i));
                } else if (type.equals(CSVUtils.CSVSchema.PropertyType.DOUBLE)) {
                    edgeProperties.put(edgePropertyColumns.get(i),new Double(edgeInfo.get(i)));
                } else if (type.equals(CSVUtils.CSVSchema.PropertyType.INT)) {
                    edgeProperties.put(edgePropertyColumns.get(i),new Integer(edgeInfo.get(i)));
                }
            }
            Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>> srctupleInfo =
                    new Tuple3<>(edgeInfo.get(1),CommonGraphComputer.VertexDirection.SRC,edgeProperties);
            Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>> dsttupleInfo =
                    new Tuple3<>(edgeInfo.get(1),CommonGraphComputer.VertexDirection.DST,edgeProperties);
            adjsList.add(srctupleInfo);
            adjsList.add(dsttupleInfo);
        }

        Map<String, Object> vertexProperties = new HashMap<>();
        List<String> vertexPropertyColumns = ArgumentUtils.toList(context.getConfiguration().get("vertexPropertyColumns"));
        String vertexPath = "file://" + context.getConfiguration().get("vertexCSVFilePath");
        Path vertexSchemaFilePath = new Path(vertexPath, "schema");
        CSVUtils.CSVSchema vertexCsvSchema = new CSVUtils.CSVSchema(vertexSchemaFilePath);

        for (Text val : vertexValues) {
            List<String> vertexInfo = ArgumentUtils.toList(val.toString());

            for (int i = 0; i < vertexPropertyColumns.size(); i++) {
                CSVUtils.CSVSchema.PropertyType type = vertexCsvSchema.getColumnType().get(vertexPropertyColumns.get(i));
                System.out.println(type);
                if (type.equals(CSVUtils.CSVSchema.PropertyType.STRING)) {
                    vertexProperties.put(vertexPropertyColumns.get(i),vertexInfo.get(i));
                } else if (type.equals(CSVUtils.CSVSchema.PropertyType.DOUBLE)) {
                    vertexProperties.put(vertexPropertyColumns.get(i),new Double(vertexInfo.get(i)));
                } else if (type.equals(CSVUtils.CSVSchema.PropertyType.INT)) {
                    vertexProperties.put(vertexPropertyColumns.get(i),new Integer(vertexInfo.get(i)));
                }
            }

        }

        Iterable<Tuple3<String, CommonGraphComputer.VertexDirection, Map<String, Object>>> adjs = iteratorToIterable(adjsList.iterator());

        String centerVertexName = key.toString();
        CommonGraphComputer.EdgeTuplesToVertexWithVProperties.ProcessAdjEdges processAdj =
                new CommonGraphComputer.EdgeTuplesToVertexWithVProperties.ProcessAdjEdges(centerVertexName, adjs,vertexProperties);
        Vertex centerVertex = processAdj.constructVertex();

        VertexWritable v = new VertexWritable(centerVertex);

        context.write(NullWritable.get(), v);
    }

}