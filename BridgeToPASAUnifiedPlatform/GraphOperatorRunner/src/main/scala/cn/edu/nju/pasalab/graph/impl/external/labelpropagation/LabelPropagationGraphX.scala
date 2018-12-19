package cn.edu.nju.pasalab.graph.impl.external.labelpropagation

import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.Constants
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.{CommonGraphComputer, CommonSerial}
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.GraphDBGraphXConverter
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.GraphSONToGraphDBGraphX
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd.{GraphDBToGraphRDD, GraphSONToGraphRDD}
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.{GraphDBToGraphSONGraphX, GraphRDDToGraphSON}
import cn.edu.nju.pasalab.graph.util.{DataBaseUtils, HDFSUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext, graphx}
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
    //result.vertices.foreach(ver => println(ver._2.get("clusterID")))
    //sc.stop()
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

  def fromGraphDBToGraphDB(arguments: util.Map[String, String]): Unit = {

    // graphdb -> graphson -> graphrdd failed
    /*val inputDBType = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_TYPE)
    val inputConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val outputConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE)
    val graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)

    val conf = DataBaseUtils.loadConfFromHDFS(inputConfPath)
    val tmpGraphSONFile = conf.getProperty("lptmpdirpath") + HDFSUtils.getTimeName

    val msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc

    GraphDBToGraphSONGraphX.converter(inputDBType,sc,inputConfPath,tmpGraphSONFile + "in/")

    arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE,tmpGraphSONFile + "out/")
    arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE, tmpGraphSONFile + "in/~g")
    fromGraphSONToGraphSON(arguments)

    arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE,tmpGraphSONFile + "out/~g")
    arguments.replace(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE,outputConfPath)
    GraphSONToGraphDBGraphX.converter(arguments)*/
    val graphComputerConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)
    val inputConfPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val inputDBType = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_TYPE)
    val resultPropertyName = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RESULT_PROPERTY_NAME)
    val graphdb = CommonSerial.createGraph(inputConfPath, inputDBType)

    val msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc

    val tmpGraphRDD = GraphDBToGraphRDD.converter(graphdb.traversal(),sc,inputConfPath)
    val result = fromGraphRDDToGraphRDD(tmpGraphRDD,resultPropertyName)
    GraphDBGraphXConverter.GraphXToGraphDB(inputConfPath,result,false)
  }

  /*def main(args: Array[String]): Unit = {

    // GraphRDDToGraphSON没问题，输入的graphSon有问题
    /*val inputGraphPath = "/home/lijunhong/graphxtosontest/directed.csv.graph"
    val graphComputerConfPath = "/home/lijunhong/graphxtosontest/SparkLocal.conf"
    val outputGraphPath = "/home/lijunhong/graphxtosontest/lptmp/tmp_181212195907out"
    val msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc

    val graph :graphx.Graph[util.HashMap[String,java.io.Serializable],
      HashMap[String,java.io.Serializable]] =
      GraphSONToGraphRDD.converter(sc,inputGraphPath)

    GraphRDDToGraphSON.converter(graph,outputGraphPath)*/
    val inputConfPath = "/home/lijunhong/graphxtosontest/Neo4j.conf"
    val graphComputerConfPath = "/home/lijunhong/graphxtosontest/SparkLocal.conf"
    val conf = DataBaseUtils.loadConfFromHDFS(inputConfPath)
    val tmpGraphSONFile = conf.getProperty("lptmpdirpath") + HDFSUtils.getTimeName

    /*val msc = new CommonGraphComputer.ManageSparkContexts(graphComputerConfPath, "Label Propagation with GraphX")
    val sc = msc.getSc*/

    val confs = new SparkConf().setMaster("local").setAppName("gremlin neo4j")
    val sc = new SparkContext(confs)


    GraphDBToGraphSONGraphX.converter(Constants.GRAPHTYPE_GRAPHDB_NEO4J,sc,inputConfPath,tmpGraphSONFile + "in/")
  }*/
}
