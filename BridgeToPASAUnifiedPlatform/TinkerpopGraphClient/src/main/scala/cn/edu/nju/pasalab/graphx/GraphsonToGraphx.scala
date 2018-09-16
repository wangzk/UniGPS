package cn.edu.nju.pasalab.graphx

import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.GREMLIN_GRAPH

import Predef._
import java.util
import java.util.{ArrayList, HashMap, Map}
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import com.google.gson.Gson
import org.apache.commons.configuration.BaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat
import org.apache.tinkerpop.gremlin.spark.structure.io.{InputFormatRDD, InputRDD}


class GraphsonToGraphx {


  def init(arguments: Map[String, String]):
  Graph[HashMap[String,String],HashMap[String,String]] = {

    //////////// Arguments
    val inputGraphFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)
    val outputGraphPath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE)

    ////////// Input graph
    val inputGraphConf = new BaseConfiguration
    inputGraphConf.setProperty(GREMLIN_GRAPH, classOf[HadoopGraph].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, classOf[GraphSONInputFormat].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphFilePath)
    inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputGraphPath)

    val manageSparkContexts = new Common.ManageSparkContexts(graphComputerConfFile, "Graphson to Graphx")
    val sc = manageSparkContexts.getSc
    val jsc = manageSparkContexts.getJsc

    val graphRDDInput = new InputFormatRDD
    val vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc)

    val vertex:RDD[(Long,HashMap[String,String])] = vertexWritableJavaPairRDD.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val vid = Hashing.sha256.hashString(v.id().toString, Charsets.UTF_8).asLong

      ///!!!!g.traversal().V(g.getStarVertex.id()).propertyMap().next()

      var graphxValueMap : HashMap[String,String] = new HashMap[String,String]()
      graphxValueMap.put("originalID",v.id().toString)

      graphxValueMap.putAll(g.traversal.V(v.id).valueMap().next(1).get(0))
      (vid,graphxValueMap)
    })

    val edgeValueMapRDD = vertexWritableJavaPairRDD.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val vid = Hashing.sha256.hashString(v.id.toString, Charsets.UTF_8).asLong
      val pathlist = g.traversal.V(v.id).out().path.by(T.id).toList
      val propertieslist = g.traversal.V(v.id).outE().valueMap().toList
      var list :List[(String,String)] = List()
      var x = 0
      for(x <- 0 until pathlist.size()){
        list = list.::(pathlist.get(x).toString,propertieslist.get(x).toString)
      }
      list
    })

    val edgeIter = edgeValueMapRDD.flatMap(list => list.iterator)

    val edge = edgeIter.map(pair => {
      val pairname = pair._1
      val gson = new Gson
      var array = new util.ArrayList[String]
      array = gson.fromJson(pairname, array.getClass)
      val md1 = Hashing.sha256.hashString(array.get(0), Charsets.UTF_8).asLong
      val md2 = Hashing.sha256.hashString(array.get(1), Charsets.UTF_8).asLong

      val properties = pair._2
      var map = new util.HashMap[String,String]
      map = gson.fromJson(properties, map.getClass)

      new Edge[HashMap[String,String]](md1, md2, map)
    })
    val graph = Graph[HashMap[String,String],HashMap[String,String]](vertex,edge,new HashMap[String,String]())
    graph
  }
}
