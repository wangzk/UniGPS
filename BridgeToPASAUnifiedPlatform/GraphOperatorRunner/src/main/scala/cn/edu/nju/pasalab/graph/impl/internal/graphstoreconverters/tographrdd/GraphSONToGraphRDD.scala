package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd

import java.util
import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.commons.configuration.BaseConfiguration
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, T, Vertex}
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.{GraphSONInputFormat, GraphSONOutputFormat}
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.spark.structure.io.{InputFormatRDD, OutputFormatRDD}

import scala.collection.JavaConverters._
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphX._


object GraphSONToGraphRDD extends Serializable {


  def converter(sc:SparkContext, inputGraphFilePath:String)
  : graphx.Graph[HashMap[String,java.io.Serializable],HashMap[String,java.io.Serializable]] = {

    ////////// Input graph
    val inputGraphConf = new BaseConfiguration
    inputGraphConf.setProperty("gremlin.graph", classOf[HadoopGraph].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, classOf[GraphSONInputFormat].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphFilePath)
    inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath)
    val jsc = JavaSparkContext.fromSparkContext(sc)
    val graphRDDInput = new InputFormatRDD
    val vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc)

    // Convert the graphRDD to vertexRDD
    val vertexRDD:RDD[(Long,HashMap[String,java.io.Serializable])] = vertexWritableJavaPairRDD.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {

      // Get the center vertex
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      // In case the vertex id in TinkerGraph is not long type
      val vid = convertStringIDToLongID(v.id().toString)

      // Pass the vertex properties to GraphX vertex value map and remain the original vertex id
      var graphxValueMap : HashMap[String,java.io.Serializable] = new HashMap[String,java.io.Serializable]()
      graphxValueMap.put("originalID",v.id().toString)
      graphxValueMap.put("originalHashID",vid)
      graphxValueMap.putAll(g.traversal.V(v.id).valueMap().next(1).get(0))
      (vid,graphxValueMap)
    })

    // Collect edges from the graphRDD
    val edge = vertexWritableJavaPairRDD.rdd.flatMap((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val edgelist:util.List[Edge] = g.traversal.V(v.id).outE().toList

      // Put all edges of the center vertex into the list
      val list = new collection.mutable.ArrayBuffer[graphx.Edge[util.HashMap[String,java.io.Serializable]]]()
      var x = 0
      for(x <- 0 until edgelist.size()){
        var srcId = edgelist.get(x).inVertex.id().toString
        var dstId = edgelist.get(x).outVertex.id().toString
        val md1 = convertStringIDToLongID(srcId)
        val md2 = convertStringIDToLongID(dstId)
        // Get the properties of the edge
        var edgeAttr = new util.HashMap[String,java.io.Serializable]()
        edgelist.get(x).properties().asScala.foreach((pro:Property[Nothing])=>
        {edgeAttr.put(pro.key(),pro.value().toString)})
        list.append(graphx.Edge(md1,md2,edgeAttr))
      }
      list
    })

    // Trim the duplicated edges
    val edgeRDD = edge.distinct()

    // Return the graph ,appoint the properties of the vertex and the edge as type HashMap
    graphx.Graph[util.HashMap[String,java.io.Serializable],
      HashMap[String,java.io.Serializable]](vertexRDD,edgeRDD,new HashMap[String,java.io.Serializable]())
  }

  def main(args: Array[String]): Unit = {
    val inputGraphFilePath = "/home/lijunhong/graphxtosontest/lptmp/tmp_181216200840out/~g"
    val sparkConf = new SparkConf().setMaster("local").setAppName("gremlin neo4j")
    val sc = new SparkContext(sparkConf)
    val graph :graphx.Graph[util.HashMap[String,java.io.Serializable],
      HashMap[String,java.io.Serializable]] = converter(sc,inputGraphFilePath)

    graph.vertices.foreach(ver => println(ver._2))


  }

}
