package cn.edu.nju.pasalab.graphx

import java.util
import java.util.{HashMap, Map}

import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.rdd.RDD
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.commons.configuration.BaseConfiguration
import org.apache.spark.api.java.JavaSparkContext
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD

import scala.collection.JavaConverters._

class GraphSONGraphXConvertor {

  def fromGraphXToGraphSON(graphRDD: graphx.Graph[util.Map[String, java.io.Serializable],
                                                  util.Map[String, java.io.Serializable]],
                           outputFilePath:String):Unit = {

  }


  def fromGraphSONToGraphX(sc:SparkContext, inputGraphFilePath:String)
  : graphx.Graph[HashMap[String,String],HashMap[String,String]] = {

    ////////// Input graph
    val inputGraphConf = new BaseConfiguration
    inputGraphConf.setProperty("gremlin.graph", classOf[HadoopGraph].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, classOf[GraphSONInputFormat].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphFilePath)
    inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath)


    val jsc = JavaSparkContext.fromSparkContext(sc)

    val graphRDDInput = new InputFormatRDD
    val vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc)

    val vertex:RDD[(Long,HashMap[String,String])] = vertexWritableJavaPairRDD.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val vid = Hashing.sha256.hashString(v.id().toString, Charsets.UTF_8).asLong

      var graphxValueMap : HashMap[String,String] = new HashMap[String,String]()
      graphxValueMap.put("originalID",v.id().toString)
      graphxValueMap.putAll(g.traversal.V(v.id).valueMap().next(1).get(0))
      (vid,graphxValueMap)
    })

    val edgeTupleRDD = vertexWritableJavaPairRDD.rdd.flatMap((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val edgelist:util.List[Edge] = g.traversal.V(v.id).outE().toList


      val list = new collection.mutable.ArrayBuffer[((Long,Long),util.HashMap[String,String])]()
      var x = 0
      for(x <- 0 until edgelist.size()){
        var srcId = edgelist.get(x).inVertex.id().toString
        var dstId = edgelist.get(x).outVertex.id().toString
        val md1 = Hashing.sha256.hashString(srcId, Charsets.UTF_8).asLong
        val md2 = Hashing.sha256.hashString(dstId, Charsets.UTF_8).asLong
        var edgeAttr = new util.HashMap[String,String]()
        edgelist.get(x).properties().asScala.foreach((pro:Property[Nothing])=>
         {edgeAttr.put(pro.key(),pro.value().toString)})
        list.append(((md1,md2),edgeAttr))
      }
      list
    })

    val edge = edgeTupleRDD.map(pair => {
      new graphx.Edge[util.HashMap[String,String]](pair._1._1,pair._1._2, pair._2)
    })

    graphx.Graph[HashMap[String,String],HashMap[String,String]](vertex,edge,new HashMap[String,String]())
  }
}
