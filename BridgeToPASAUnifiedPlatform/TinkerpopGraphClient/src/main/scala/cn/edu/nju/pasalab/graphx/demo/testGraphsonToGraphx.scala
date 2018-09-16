package cn.edu.nju.pasalab.graphx.demo

import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.Constants
import cn.edu.nju.pasalab.graphx.GraphsonToGraphx
import org.apache.spark.graphx.Graph

object testGraphsonToGraphx {
  def main(args: Array[String]): Unit = {
    val inputCSVFile = "/home/lijunhong/test.csv"
    val graphComputerPath = "/home/lijunhong/IdeaProjects/GraphOperator/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/graph-computer/SparkLocal.conf"

    val arguments: util.Map[String, String] = new HashMap[String, String]
    arguments.put(Constants.ARG_INPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON)
    arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph")
    arguments.put(Constants.ARG_RESULT_PROPERTY_NAME, "clusterID")
    arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_GRAPHX)
    arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerPath)
    arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON)
    arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".aftergraphxpr")
    val gx = new GraphsonToGraphx
    val result:Graph[HashMap[String,String],HashMap[String,String]] = gx.init(arguments)
    result.edges.foreach(println)
  }
}
