package cn.edu.nju.pasalab.graph.impl.external.labelpropagation

import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd.GraphSONToGraphRDD
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphRDDToGraphSON
import cn.edu.nju.pasalab.graph.util.HDFSUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx
import org.apache.spark.graphx.lib.LabelPropagation

object LabelPropagationGraphX {

  def fromGraphSONToGraphSON(arguments: util.Map[String, String]): Unit = {
    val inputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val outputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE)
    val resultPropertyName = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RESULT_PROPERTY_NAME)
    val graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)
    val fs = HDFSUtils.getFS(outputGraphPath)
    fs.delete(new Path(outputGraphPath), true)

    val msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc

    val graph :graphx.Graph[util.HashMap[String,java.io.Serializable],
      HashMap[String,java.io.Serializable]] =
      GraphSONToGraphRDD.converter(sc,inputGraphPath)

    val result = fromGraphRDDToGraphRDD(graph,resultPropertyName)
    GraphRDDToGraphSON.converter(result,outputGraphPath)

  }


  def fromGraphRDDToGraphRDD(graph :graphx.Graph[util.HashMap[String,java.io.Serializable],
                                  HashMap[String,java.io.Serializable]],
                             propertyName : String)
  : graphx.Graph[util.HashMap[String,java.io.Serializable],
    HashMap[String,java.io.Serializable]] = {

    val graphafterlp = LabelPropagation.run(graph,11)
    val resultVertices = graphafterlp.vertices.map(tuple =>{
      val vertexID = tuple._1
      val clusterID = tuple._2
      val attr :util.HashMap[String,java.io.Serializable] = new util.HashMap
      attr.put(propertyName,clusterID)
      (vertexID,attr)
    } )
    val result = graph.joinVertices(resultVertices){
      (vid,attrs,cluster) => attrs.putAll(cluster)
        attrs
    }
    result
  }
}
