package cn.edu.nju.pasalab.graphx

import scala.collection.JavaConversions
import Predef._
import java.util
import java.util.HashMap
import java.lang
import java.util.ArrayList

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.Function
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}

import scala.collection.mutable.Map
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

import scala.reflect.ClassTag


class ToGraphx {


  def initGraphxFromJavapair(javapair: JavaPairRDD[AnyRef, VertexWritable]): Unit = {


    val vertex:RDD[(Long,HashMap[String,String])] = javapair.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val vid = Hashing.sha256.hashString(v.id().toString, Charsets.UTF_8).asLong

      ///!!!!g.traversal().V(g.getStarVertex.id()).propertyMap().next()

      var graphxValueMap : HashMap[String,String] = new HashMap[String,String]()
      graphxValueMap.put("originalID",v.id().toString)

      graphxValueMap.putAll(g.traversal.V(v.id).valueMap().next(1).get(0))
      (vid,graphxValueMap)
    })

    val edgeValueMapRDD = javapair.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
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
    graph.vertices.foreach(println)
    //vertex.foreach(println)


/*    var tinkergraph = TinkerGraph.open
    val tg = tinkergraph.traversal()
    graph.vertices.collect().foreach(line => {
      val jsonString = line._2
      val gson = new Gson
      var map = new util.HashMap[String, AnyRef]
      map = gson.fromJson(jsonString, map.getClass)

      tg.addV("SimpleV").property(T.id,line._1).iterate()
      var idx:lang.Long = line._1
      map.foreach(pair =>{
        tg.V(idx).property(pair._1,pair._2.toString.replace("[", "").replace("]", ""))
          .iterate()
      })
    })
    tg.V().valueMap(true).toList.foreach(println)
    var edgeIdx :lang.Integer = 0
    graph.edges.collect().foreach(triple => {
      var src:lang.Long = triple.srcId
      var dst:lang.Long = triple.dstId
      val jsonString = triple.attr
      val gson = new Gson
      var map = new util.HashMap[String, AnyRef]
      map = gson.fromJson(jsonString, map.getClass)

      tg.V(src).as("a").V(dst).as("b").addE("SimpleE")
        .property(T.id,edgeIdx)
        .from("a").to("b").iterate()

      map.foreach(pair => {
        tg.E(edgeIdx).property(pair._1,pair._2.toString).iterate()
      })
      edgeIdx = edgeIdx + 1
    })
    //tg.E().valueMap(true).toList.foreach(println)

    tinkergraph*/
/*
/*    val result = LabelPropagation.run(graph,11)
    result.vertices.foreach(println)*/

    var count = 0

    for(x <- 0 until 10){
      tg.addV().property(T.id,count).iterate()
      count = count + 1
    }
    tg.V().toList.foreach(println)

*/

  }
}
