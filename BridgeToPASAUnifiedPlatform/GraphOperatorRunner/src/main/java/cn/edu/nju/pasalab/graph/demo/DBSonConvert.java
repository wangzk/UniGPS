package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.GraphSONToGraphDBGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphDBToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.util.DataBaseUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DBSonConvert {
    public static void main(String[] args) throws IOException, ConfigurationException {
        Map<String, String> arguments = new HashMap<>();
        String graphComputerConfFile = "/home/lijunhong/graphxtosontest/SparkLocal.conf";
        String dbConfFilePath = "/home/lijunhong/graphxtosontest/Neo4j.conf";
        arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE,"/home/lijunhong/graphxtosontest/test.csv.graph");
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE,dbConfFilePath);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE,Constants.GRAPHTYPE_GRAPHDB_NEO4J);
        GraphSONToGraphDBGraphX.converter(arguments);
        /*CommonGraphComputer.ManageSparkContexts manageSparkContexts1 = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB");
        SparkContext sc1 = manageSparkContexts1.getSc();
        sc1.stop();*/


        Properties conf = DataBaseUtils.loadConfFromHDFS(dbConfFilePath);
        String tmpDirPath = conf.getProperty("tmpdirpath");
        String tmpFilePath = tmpDirPath + HDFSUtils.getTimeName();
        String dbType = Constants.GRAPHTYPE_GRAPHDB_NEO4J;
        CommonGraphComputer.ManageSparkContexts manageSparkContexts = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB");
        SparkContext sc = manageSparkContexts.getSc();
        //GraphDBToGraphSONGraphX.converter(dbType, sc,dbConfFilePath,tmpFilePath + "~/g~");
        GraphDBToGraphSONGraphX.converter(dbType, sc,dbConfFilePath,tmpFilePath);
    }
}
