package cn.edu.nju.pasalab.graphx.demo

import java.io.Serializable
import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.Constants
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common
import cn.edu.nju.pasalab.graphx.GraphSONGraphXConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.Graph

object testGraphsonToGraphx {
  def main(args: Array[String]): Unit = {
    val inputCSVFile = "/home/lijunhong/my-graph.json"
    val graphComputerPath = "./conf/graph-computer/SparkLocal.conf"

    val manageSparkContexts = new Common.ManageSparkContexts(graphComputerPath, "GraphSON File to Graphx")
    val sc = manageSparkContexts.getSc
    val jsc = manageSparkContexts.getJsc
    val gx = new GraphSONGraphXConverter
    val result:Graph[util.HashMap[String,java.io.Serializable],util.HashMap[String,java.io.Serializable]] = gx.fromGraphSONToGraphX(sc,inputCSVFile)
    //result.edges.foreach(println)
    //result.triplets.foreach(println)
    gx.fromGraphXToGraphSON(result,"/home/lijunhong/aftertrans.graph")

  }
}
