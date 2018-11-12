package cn.edu.nju.pasalab.graph.impl.graphx

import java.util
import java.util.{HashMap, Map}

import cn.edu.nju.pasalab.graph.impl.HDFSUtils
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.lib.LabelPropagation


object GopLabelPropagation {
  def fromGraphSONToGraphSON(arguments: util.Map[String, String]): Unit = {
    val inputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val outputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE)
    val resultPropertyName = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RESULT_PROPERTY_NAME)
    val graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)
    val fs = HDFSUtils.getFS(outputGraphPath)
    fs.delete(new Path(outputGraphPath), true)

    val msc = new Common.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc

    val graph :graphx.Graph[util.HashMap[String,java.io.Serializable],
      HashMap[String,java.io.Serializable]] = cn.edu.nju.pasalab.graphx.GraphSONGraphXConverter.fromGraphSONToGraphX(sc,inputGraphPath)

    val graphafterlp = LabelPropagation.run(graph,11)
    val resultVertices = graphafterlp.vertices.map(tuple =>{
      val vertexID = tuple._1
      val clusterID = tuple._2
      val attr :util.HashMap[String,java.io.Serializable] = new util.HashMap
      attr.put(resultPropertyName,clusterID)
      (vertexID,attr)
    } )
    val result = graph.joinVertices(resultVertices){
      (vid,attrs,cluster) => attrs.putAll(cluster)
        attrs
    }
    cn.edu.nju.pasalab.graphx.GraphSONGraphXConverter.fromGraphXToGraphSON(result,outputGraphPath)
  }
}
