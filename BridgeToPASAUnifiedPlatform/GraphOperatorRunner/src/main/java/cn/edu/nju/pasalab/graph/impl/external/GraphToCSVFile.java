package cn.edu.nju.pasalab.graph.impl.external;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv.GraphSONToGraphCSVGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv.GraphSONToGraphCSVSerial;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.GraphSONToGraphDBGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.graphsontographdb.GraphSONToGraphDBSerial;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphDBToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphdbtographson.GraphDBToGraphSONSerial;
import cn.edu.nju.pasalab.graph.util.DataBaseUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.Map;
import java.util.Properties;

public class GraphToCSVFile {

    public static void fromGraphSON(Map<String, String> arguments) throws Exception {
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_SERIAL)) {
            GraphSONToGraphCSVSerial.converter(arguments);
        } else if (runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            GraphSONToGraphCSVGraphX.converter(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }

    public static void fromGraphDB(Map<String, String> arguments) throws Exception {

        String dbConfFilePath = arguments.get(Constants.ARG_INPUT_GRAPH_CONF_FILE);
        ///create tmp file path for the GraphSON, first convert the GraphDB to GraphSON
        Properties conf = DataBaseUtils.loadConfFromHDFS(dbConfFilePath);
        String tmpDirPath = conf.getProperty("tmpdirpath");
        String tmpFilePath = tmpDirPath + HDFSUtils.getTimeName();


        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, tmpFilePath);
        //arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, "/home/lijunhong/graphxtosontest/vertex.csv/graphson");
        String dbType = arguments.get(Constants.ARG_INPUT_GRAPH_TYPE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_SERIAL)) {
            GraphDBToGraphSONSerial.converter(dbConfFilePath,dbType,tmpFilePath);
        } else if (runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("gremlin neo4j");
            SparkContext sc = new SparkContext(sparkConf);
            GraphDBToGraphSONGraphX.converter(dbType, sc,dbConfFilePath,tmpFilePath);
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

        arguments.replace(Constants.ARG_INPUT_GRAPH_CONF_FILE, tmpFilePath);
        fromGraphSON(arguments);

    }
}
