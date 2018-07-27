package cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer;

import cn.edu.nju.pasalab.graph.impl.util.CSVUtils;
import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils;
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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_GRAPH;

public class GopVertexPropertiesToCSVFile {

    public static void fromGraphSON(Map<String, Object> arguments) throws Exception {
        //////////// Arguments
        // For Gryo graph file, the conf path is the graph file path
        String inputGraphFilePath = (String)arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        List<String> properties = (List<String>)arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_PROPERTIES);
        String outputCSVFilePath = (String)arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_VERTEX_CSV_FILE_PATH);
        String graphComputerConfFile = (String)arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);

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
        Common.ManageSparkContexts manageSparkContexts = new Common.ManageSparkContexts(graphComputerConfFile, "Gryo Vertex File to CSV File");
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
}
