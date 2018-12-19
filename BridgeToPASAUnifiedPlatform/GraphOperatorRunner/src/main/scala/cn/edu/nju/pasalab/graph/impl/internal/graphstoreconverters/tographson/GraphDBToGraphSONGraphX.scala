package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson

import java.io.PrintWriter
import java.net.URI
import java.util.Properties
import java.{lang, util}

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphX._
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.{CommonGraphComputer, CommonSerial}
import cn.edu.nju.pasalab.graph.util.{DataBaseUtils, HDFSUtils}
import org.apache.commons.configuration.BaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, T, Vertex, VertexProperty}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GraphDBToGraphSONGraphX {
  def converter(outputGraphType: String, sc: SparkContext,
                dbConfFilePath: String,
                outputFilePath: String)
  : Unit = {

    val graphdb = CommonSerial.createGraph(dbConfFilePath, outputGraphType)
    val g = graphdb.traversal()
    val conf = DataBaseUtils.loadConfFromHDFS(dbConfFilePath)
    val tmpDirPath = conf.getProperty("tmpdirpath")
    val minPartition: Int = conf.getProperty("minpartition").toInt

    val tmpFilePath = tmpDirPath + HDFSUtils.getTimeName
    val tmpConf = new Configuration()
    val tmpFs = org.apache.hadoop.fs.FileSystem.get(tmpConf)
    val tmpOutput = tmpFs.create(new Path(tmpFilePath))
    val tmpWriter = new PrintWriter(tmpOutput)

    val traversal = g.V().id()
    while(traversal.hasNext){
      tmpWriter.write(traversal.next().toString +"\n")
    }
    tmpWriter.close()

    val vertexlist = sc.textFile(tmpFilePath,minPartition).cache()

    g.close()

    val vertexRDD = vertexlist.mapPartitions(vertexIter => {
      val g = createDBClient(conf).openDB().traversal()
      val res = ArrayBuffer[StarGraph#StarVertex]()
      while (vertexIter.hasNext){
        // Create the StarGraph and its center
        val graph = StarGraph.open
        val vertexCache = new util.HashMap[Long, Vertex]
        val edgeCache = new util.HashMap[lang.Long, Edge]
        val dbVertexID = vertexIter.next()
        val dbVertex = g.V(dbVertexID).next()
        val centerVertexID= convertStringIDToLongID(dbVertexID)
        val dbVertexAttr: java.util.Iterator[VertexProperty[Nothing]] = dbVertex.properties()
        val centerVertexAttr = new util.HashMap[String,java.io.Serializable]()
        dbVertexAttr.foreach(itr => {
          centerVertexAttr.put(itr.key(),itr.value().toString)
        })
        val centerVertex:Vertex = getOrCreateVertexForStarGraph(graph,vertexCache,
          centerVertexID,true,centerVertexAttr)

        // Add adjacent edges
        dbVertex.edges(Direction.IN).foreach(edge => {
          val anotherVertexID = convertStringIDToLongID(edge.outVertex().id().toString)
          val dbEdgeProperties = edge.properties()
          val srcV = centerVertex
          val dstV :Vertex = getOrCreateVertexForStarGraph(graph,vertexCache,anotherVertexID,false, null)
          val edgeProperties = new util.HashMap[String,java.io.Serializable]()

          dbEdgeProperties.foreach(itr => {
            edgeProperties.put(itr.key(),itr.value().toString)
          })
          // For both direction, add an edge between the both vertices
          val outedgeID:lang.Long = hashEdgeID(edge.outVertex().id().toString,edge.inVertex().toString)
          val outedge = getOrCreateEdgeForStarGraph(dstV,edgeCache,srcV,outedgeID,edgeProperties)

          val inedgeID:lang.Long = hashEdgeID(edge.inVertex().id().toString,edge.outVertex().id().toString)
          val inedge = getOrCreateEdgeForStarGraph(srcV,edgeCache,dstV,inedgeID,edgeProperties)
        })
        res.append(graph.getStarVertex)
      }
      g.close()
      res.iterator
    })

    val tinkergraphRDD = vertexRDD.map(vertex => (AnyRef :AnyRef, new VertexWritable(vertex))).toJavaRDD()
    println(tinkergraphRDD.rdd.count())

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
    //println(outputConf.getProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))
    formatRDD.writeGraphRDD(outputConf, JavaPairRDD.fromJavaRDD(tinkergraphRDD))
    sc.stop()
    FileSystem.get(path,hadoopConf).rename(new Path(tmpOutputPath, "~g"), new Path(outputFilePath))
    FileSystem.get(path,hadoopConf).delete(new Path(tmpOutputPath), true)
  }

  def main(args: Array[String]): Unit = {
    val graphComputerConfFile = "/home/lijunhong/graphxtosontest/SparkLocal.conf"
    val dbConfFilePath = "/home/lijunhong/graphxtosontest/Neo4j.conf"
    val manageSparkContexts1 = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB")
    val sc1 = manageSparkContexts1.getSc
    sc1.stop()


    val tmpFilePath = "/home/lijunhong/graphxtosontest/tmp"
    val manageSparkContexts = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB")
    val sc = manageSparkContexts.getSc
    val outputConf = new BaseConfiguration
    val tmpOutputPath = tmpFilePath + "~"
    val hadoopConf = new Configuration
    val path = URI.create(tmpOutputPath)
    outputConf.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, tmpOutputPath)
    println(outputConf.getProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))
    sc.stop()
    //FileSystem.get(path,hadoopConf).delete(new Path(tmpOutputPath), true)
    //FileSystem.get(path,hadoopConf).rename(new Path(tmpOutputPath), new Path(tmpFilePath))

  }
}
