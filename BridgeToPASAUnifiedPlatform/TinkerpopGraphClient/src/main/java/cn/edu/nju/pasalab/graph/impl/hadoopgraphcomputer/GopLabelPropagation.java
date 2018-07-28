package cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer;

import cn.edu.nju.pasalab.graph.impl.util.ConfUtils;
import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;

import java.util.Map;

import static cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_GRAPH;
import static cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_TMP_GRAPH_DIR_NAME;

public class GopLabelPropagation {

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
