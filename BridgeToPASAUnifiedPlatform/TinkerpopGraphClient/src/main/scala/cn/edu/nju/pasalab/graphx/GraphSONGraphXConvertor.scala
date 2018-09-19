package cn.edu.nju.pasalab.graphx

import java.util
import java.lang
import java.util.{HashMap, Map}

import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common
import cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer.Common.VertexDirection
import cn.edu.nju.pasalab.graph.impl.util.HDFSUtils
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.rdd.RDD
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.graphx.{EdgeDirection, VertexRDD}
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, T, Vertex}
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.{GraphSONInputFormat, GraphSONOutputFormat}
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.spark.structure.io.{InputFormatRDD, OutputFormatRDD}

import scala.collection.JavaConverters._




class GraphSONGraphXConvertor extends Serializable {

  val DEFAULT_VERTEX_LABEL = "SimpleV"
  val DEFAULT_EDGE_LABEL = "SimpleE"

  def convertStringIDToLongID(id:String):Long = {
    Math.abs(Hashing.sha256.hashString(id, Charsets.UTF_8).asLong())
  }

  def hashEdgeID(src:String,dst:String,isOut:Boolean):Long = {
    if(isOut){
      Math.abs(Hashing.sha256.hashString(src+dst,Charsets.UTF_8).asLong())
    }else{
      Math.abs(Hashing.sha256.hashString(dst+src,Charsets.UTF_8).asLong())
    }
  }

  def getOrCreateVertexForStarGraph(graph:StarGraph, cache:util.HashMap[Long, Vertex],
                      name: Long,isStar: Boolean,
                      properties :util.HashMap[String, java.io.Serializable]):Vertex = {

    if (cache.containsKey(name) && !isStar) cache.get(name)
    else if (!cache.containsKey(name) && !isStar){
      val v = graph.addVertex(T.id, name:lang.Long, T.label, DEFAULT_VERTEX_LABEL)
      cache.put(name, v)
      v
    } else if (cache.containsKey(name) && isStar) {
      val v = cache.get(name)
      properties.asScala.foreach(pro => {
        v.property(pro._1, pro._2)
      })
      cache.replace(name, v)
      v
    } else {
      val v = graph.addVertex(T.id, name:lang.Long, T.label, DEFAULT_VERTEX_LABEL)
      properties.asScala.foreach(pro => {
        v.property(pro._1, pro._2)
      })
      cache.put(name, v)
      v
    }
  }

  def addProperties(e: Edge, properties: util.HashMap[String, java.io.Serializable]) = {
    properties.asScala.foreach(pro => {
      e.property(pro._1, pro._2)
    })
    e
  }


  def fromGraphXToGraphSON(graphRDD: graphx.Graph[util.HashMap[String, java.io.Serializable],
                                                  util.HashMap[String, java.io.Serializable]],
                           outputFilePath:String):Unit = {
    val sc = graphRDD.edges.sparkContext

    //val vertexRDD : RDD[Vertex] = graphRDD.triplets.map(triple => {
//      ((triple.srcId,triple.srcAttr),triple.dstId,triple.dstAttr,triple.attr)
//}).groupBy(_._1).map(edgeTuples => {
    //val vertexRDD: RDD[Vertex] = graphRDD.collectEdges(EdgeDirection.Out)
    val vertexRDDWithEdgeProperties = graphRDD.collectEdges(EdgeDirection.Out)

    val tinkerPopVertexRDD = graphRDD.outerJoinVertices(vertexRDDWithEdgeProperties) {
      case (centerVertexName, centerVertexAttr, adjs) => {

        val graph = StarGraph.open
        val cache = new util.HashMap[Long, Vertex]
        val centerVertex:Vertex = getOrCreateVertexForStarGraph(graph,cache,
                                                                centerVertexName,true,centerVertexAttr)
        // Add adjacency edges
        adjs.get.map(edge => {

          val anotherVertexName = edge.dstId
          val edgeProperties = edge.attr
          val srcV = centerVertex
          val dstV :Vertex = getOrCreateVertexForStarGraph(graph,cache,anotherVertexName,false, null)

          val outedgeID:lang.Long = hashEdgeID(edge.srcId.toString,edge.dstId.toString,true)
          val outedge = srcV.addEdge(DEFAULT_EDGE_LABEL,dstV,T.id,outedgeID)
          if (outedge != null && edgeProperties.size > 0) addProperties(outedge, edgeProperties)

          val inedgeID:lang.Long = hashEdgeID(edge.srcId.toString,edge.dstId.toString,false)
          val inedge = dstV.addEdge(DEFAULT_EDGE_LABEL,srcV,T.id,inedgeID)
          if (inedge != null && edgeProperties.size > 0) addProperties(inedge, edgeProperties)

        })

        graph.getStarVertex
      }
    }.vertices.map {case(vid, vertex) => vertex}

    val tinkergraphRDD = tinkerPopVertexRDD.map(vertex => (AnyRef :AnyRef, new VertexWritable(vertex))).toJavaRDD()


    ///////// Output the VertexRDD

    val outputConf = new BaseConfiguration
    val tmpOutputPath = outputFilePath + "~"
    outputConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, classOf[GraphSONOutputFormat].getName)
    outputConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, tmpOutputPath)
    HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true)
    HDFSUtils.getFS(outputFilePath).delete(new Path(outputFilePath), true)
    HDFSUtils.getFS(outputFilePath).deleteOnExit(new Path(tmpOutputPath))
    val formatRDD = new OutputFormatRDD
    formatRDD.writeGraphRDD(outputConf, JavaPairRDD.fromJavaRDD(tinkergraphRDD))
    sc.stop()
    HDFSUtils.getFS(outputFilePath).rename(new Path(tmpOutputPath, "~g"), new Path(outputFilePath))
    HDFSUtils.getFS(outputFilePath).delete(new Path(tmpOutputPath), true)
  }


  def fromGraphSONToGraphX(sc:SparkContext, inputGraphFilePath:String)
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

    val vertex:RDD[(Long,HashMap[String,java.io.Serializable])] = vertexWritableJavaPairRDD.rdd.map((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val vid = convertStringIDToLongID(v.id().toString)

      var graphxValueMap : HashMap[String,java.io.Serializable] = new HashMap[String,java.io.Serializable]()
      graphxValueMap.put("originalID",v.id().toString)
      graphxValueMap.putAll(g.traversal.V(v.id).valueMap().next(1).get(0))
      (vid,graphxValueMap)
    })

    val edgeRDD = vertexWritableJavaPairRDD.rdd.flatMap((tuple2: Tuple2[AnyRef, VertexWritable]) => {
      val v = tuple2._2.get
      val g = StarGraph.of(v)
      val edgelist:util.List[Edge] = g.traversal.V(v.id).outE().toList


      val list = new collection.mutable.ArrayBuffer[graphx.Edge[util.HashMap[String,java.io.Serializable]]]()
      var x = 0
      for(x <- 0 until edgelist.size()){
        var srcId = edgelist.get(x).inVertex.id().toString
        var dstId = edgelist.get(x).outVertex.id().toString
        val md1 = convertStringIDToLongID(srcId)
        val md2 = convertStringIDToLongID(dstId)
        var edgeAttr = new util.HashMap[String,java.io.Serializable]()
        edgelist.get(x).properties().asScala.foreach((pro:Property[Nothing])=>
        {edgeAttr.put(pro.key(),pro.value().toString)})
        list.append(graphx.Edge(md1,md2,edgeAttr))
      }
      list
    })

    val edge = edgeRDD.distinct()
    graphx.Graph[HashMap[String,java.io.Serializable],HashMap[String,java.io.Serializable]](vertex,edge,new HashMap[String,java.io.Serializable]())
  }


}
