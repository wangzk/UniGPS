package cn.edu.nju.pasalab.graph.impl.external.labelpropagation;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.GraphSONToGraphDBGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphDBToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.util.ConfUtils;
import cn.edu.nju.pasalab.graph.util.DataBaseUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH;
import static cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_TMP_GRAPH_DIR_NAME;

public class LabelPropagationGraphComputer {

    public static void fromGraphSONToGraphSON(Map<String, String> arguments) throws Exception {
        String inputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        String outputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
        String resultPropertyName = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RESULT_PROPERTY_NAME);
        String graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);
        FileSystem fs = HDFSUtils.getFS(outputGraphPath);
        fs.delete(new Path(outputGraphPath), true);
        Configuration graphComputerConf = ConfUtils.loadConfFromHDFS(graphComputerConfPath);
        graphComputerConf.setProperty(GREMLIN_GRAPH, HadoopGraph.class.getName());
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphPath);
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getName());
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputGraphPath);
        graphComputerConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GraphSONOutputFormat.class.getName());
        graphComputerConf.setProperty (Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, "true");
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

    public static void fromGraphDBToGraphDB(Map<String, String> arguments) throws Exception {
        String inputDBType = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_TYPE);
        String inputConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE);
        String outputConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
        String graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE);

        Properties conf = DataBaseUtils.loadConfFromHDFS(inputConfPath);
        String tmpGraphSONFile = conf.getProperty("lptmpdirpath") + HDFSUtils.getTimeName();

        CommonGraphComputer.ManageSparkContexts msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphComputer");
        SparkContext sc = msc.getSc();

        GraphDBToGraphSONGraphX.converter(inputDBType,sc,inputConfPath,tmpGraphSONFile + "in/");

        arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE,tmpGraphSONFile + "out");
        arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE, tmpGraphSONFile + "in/~g");
        fromGraphSONToGraphSON(arguments);

        arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE,tmpGraphSONFile + "out");
        arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE,outputConfPath);
        GraphSONToGraphDBGraphX.converter(arguments);
    }

}