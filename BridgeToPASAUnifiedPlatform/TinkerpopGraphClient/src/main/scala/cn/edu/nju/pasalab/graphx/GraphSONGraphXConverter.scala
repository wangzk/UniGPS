package cn.edu.nju.pasalab.graphx

import java.util
import java.lang
import java.net.URI
import java.util.HashMap

import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.rdd.RDD
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.apache.spark.graphx.EdgeDirection
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, T, Vertex}
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.{GraphSONInputFormat, GraphSONOutputFormat}
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.spark.structure.io.{InputFormatRDD, OutputFormatRDD}

import scala.collection.JavaConverters._




class GraphSONGraphXConverter extends Serializable {

  val DEFAULT_VERTEX_LABEL = "SimpleV"
  val DEFAULT_EDGE_LABEL = "SimpleE"

  def convertStringIDToLongID(id:String):Long = {
    Math.abs(Hashing.sha256.hashString(id, Charsets.UTF_8).asLong())
  }

  def hashEdgeID(src:String,dst:String):Long = {
    Math.abs(Hashing.sha256.hashString(src+dst,Charsets.UTF_8).asLong())
  }

  def getOrCreateVertexForStarGraph(graph:StarGraph, cache:util.HashMap[Long, Vertex],
                      name: Long,isCenter: Boolean,
                      properties :util.HashMap[String, java.io.Serializable]):Vertex = {

    // Get the vertex contained in the cache or create one
    // Return the vertex
    if (cache.containsKey(name) && !isCenter) cache.get(name)
    else if (!cache.containsKey(name) && !isCenter) {
      val v = graph.addVertex(T.id, name:lang.Long, T.label, DEFAULT_VERTEX_LABEL)
      cache.put(name, v)
      v
    } else if (cache.containsKey(name) && isCenter) {
      val v = cache.get(name)

      // Add the properties only if the vertex is center vertex
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
    // Get the sparkcontext
    val sc = graphRDD.edges.sparkContext

    // Tuple2 of the src vertex id and the array of all its out Edge
    val vertexRDDWithEdgeProperties = graphRDD.collectEdges(EdgeDirection.Out)

    // Join the vertex id ,vertex attribute and the array of all its out edges(as adjacent edges)
    val tinkerPopVertexRDD = graphRDD.outerJoinVertices(vertexRDDWithEdgeProperties) {
      case (centerVertexID, centerVertexAttr, adjs) => {
        // Create the StarGraph and its center
        val graph = StarGraph.open
        val cache = new util.HashMap[Long, Vertex]
        val centerVertex:Vertex = getOrCreateVertexForStarGraph(graph,cache,
                                                                centerVertexID,true,centerVertexAttr)
        // Add adjacent edges
        adjs.get.map(edge => {

          // Create the adjacent vertex
          val anotherVertexID = edge.dstId
          val edgeProperties = edge.attr
          val srcV = centerVertex
          val dstV :Vertex = getOrCreateVertexForStarGraph(graph,cache,anotherVertexID,false, null)

          // For both direction, add an edge between the both vertices
          val outedgeID:lang.Long = hashEdgeID(edge.srcId.toString,edge.dstId.toString)
          val outedge = srcV.addEdge(DEFAULT_EDGE_LABEL,dstV,T.id,outedgeID)
          if (outedge != null && edgeProperties.size > 0) addProperties(outedge, edgeProperties)

          val inedgeID:lang.Long = hashEdgeID(edge.dstId.toString,edge.srcId.toString)
          val inedge = dstV.addEdge(DEFAULT_EDGE_LABEL,srcV,T.id,inedgeID)
          if (inedge != null && edgeProperties.size > 0) addProperties(inedge, edgeProperties)
        })

        // Return the center vertex
        graph.getStarVertex
      }
    }.vertices.map {case(vid, vertex) => vertex}

    // Change the form for adapting to the java interface
    val tinkergraphRDD = tinkerPopVertexRDD.map(vertex => (AnyRef :AnyRef, new VertexWritable(vertex))).toJavaRDD()


    ///////// Output the VertexRDD
    val outputConf = new BaseConfiguration
    val tmpOutputPath = outputFilePath + "~"
    val hadoopConf = new Configuration
    val path = URI.create(outputFilePath)
    outputConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, classOf[GraphSONOutputFormat].getName)
    outputConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, tmpOutputPath)
    FileSystem.get(path,hadoopConf).delete(new Path(tmpOutputPath), true)
    FileSystem.get(path,hadoopConf).delete(new Path(outputFilePath), true)
    FileSystem.get(path,hadoopConf).deleteOnExit(new Path(tmpOutputPath))
    val formatRDD = new OutputFormatRDD
    formatRDD.writeGraphRDD(outputConf, JavaPairRDD.fromJavaRDD(tinkergraphRDD))
    sc.stop()
    FileSystem.get(path,hadoopConf).rename(new Path(tmpOutputPath, "~g"), new Path(outputFilePath))
    FileSystem.get(path,hadoopConf).delete(new Path(tmpOutputPath), true)
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


}
