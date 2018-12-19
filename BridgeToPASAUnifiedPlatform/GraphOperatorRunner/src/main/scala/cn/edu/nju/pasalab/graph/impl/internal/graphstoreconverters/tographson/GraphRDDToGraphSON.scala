package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson
import java.util
import java.lang
import java.net.URI

import org.apache.spark.{SparkContext, graphx}

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

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphX._

object GraphRDDToGraphSON extends Serializable {

  def converter(graphRDD: graphx.Graph[util.HashMap[String, java.io.Serializable],
    util.HashMap[String, java.io.Serializable]],
                           outputFilePath:String):Unit = {
    // Get the sparkcontext
    val sc = graphRDD.edges.sparkContext

    //graphRDD.vertices.foreach(ver => println(ver._1))
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

}
