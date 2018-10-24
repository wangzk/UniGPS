package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters

import java.io.{IOException, Serializable}
import java.util
import java.util.Properties

import cn.edu.nju.pasalab.graph.util.DBClient.client.IClient
import cn.edu.nju.pasalab.graph.util.DBClient.factory.{Neo4jClientFactory, OrientDBClientFactory}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import CommonGraphX._
import cn.edu.nju.pasalab.graph.util.DataBaseUtils

object GraphDBGraphXConverter extends Serializable{

  val DEFAULT_VERTEX_LABEL = "SimpleV"
  val DEFAULT_EDGE_LABEL = "SimpleE"

  /**
    * Create the database client according to the configuration.
    * @param conf is a java.util.Properties
    */
  private def createClient(conf: Properties): IClient ={
    val dbType = conf.getProperty("type")
    dbType match {
      case "orientdb" =>
        val factory = new OrientDBClientFactory
        factory.createClient(conf)
      case "neo4j" =>
        val factory = new Neo4jClientFactory
        factory.createClient(conf)
      case _ => throw new IOException("No implementation for graph type:" + conf.getProperty("type"))
    }
  }

  /**
    * Commit if g supports transactions. Otherwise, do nothing.
    * @param g a graph traversal source in TinkerPop
    */
  private def safeCommit(g: GraphTraversalSource): Unit = {
    if (g.getGraph.features().graph().supportsTransactions()) g.tx().commit()
  }

  private def getLabel(attr: util.Map[String, java.io.Serializable],
                       isCustomized: Boolean,
                       label: String,
                       default: String): String = {
    if(isCustomized) label
    else if(attr.containsKey("label")){
      attr.remove("label").toString
    }else{
      default
    }
  }
  def GraphDBToGraphX(g: GraphTraversalSource,sc: SparkContext)
  : graphx.Graph[util.Map[String, java.io.Serializable],
    util.Map[String, java.io.Serializable]] ={

    val edgelist = g.E().toList.toSeq

    val graphxEdge = edgelist.map(edge => {
      val srcId = edge.inVertex.id().toString
      val dstId = edge.outVertex.id().toString
      val md1 = convertStringIDToLongID(srcId)
      val md2 = convertStringIDToLongID(dstId)
      // Add the properties of the edge
      val edgeAttr: util.Map[String,java.io.Serializable] = new util.HashMap[String,java.io.Serializable]()
      edgeAttr.put("label",edge.label())
      edgeAttr.put("originalID",edge.id().toString)
      val attr: util.Iterator[Property[Nothing]] = edge.properties()
      attr.foreach(itr => {
        edgeAttr.put(itr.key(),itr.value().toString)
      })
      graphx.Edge(md1,md2,edgeAttr)
    })

    // Same process as the edges above
    val vertexlist = g.V().toList.toSeq

    val graphxVertex = vertexlist.map(vertex => {
      val vertexId = vertex.id().toString
      val md = convertStringIDToLongID(vertexId)
      val vertexAttr: util.Map[String,java.io.Serializable] = new util.HashMap[String,java.io.Serializable]()
      vertexAttr.put("label",vertex.label())
      vertexAttr.put("originalID",vertexId)
      val attr: java.util.Iterator[VertexProperty[Nothing]] = vertex.properties()
      attr.foreach(itr => {
        vertexAttr.put(itr.key(),itr.value().toString)
      })
      (md,vertexAttr)
    })

    // Make the RDD for GraphX
    val vertexRDD = sc.parallelize(graphxVertex)
    val edgeRDD = sc.parallelize(graphxEdge)

    // Appoint the type of the vertex and edge attributes
    graphx.Graph[util.Map[String,java.io.Serializable],
      util.Map[String,java.io.Serializable]](vertexRDD,edgeRDD,new util.HashMap[String,java.io.Serializable]())
  }


  def GraphXToGraphDB(dbConfFilePath: String,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      doClear: Boolean)
  : Unit = {
    GraphXToGraphDB(dbConfFilePath,null,graph,isLabelCustomized = false,"","",doClear)
  }

  def GraphXToGraphDB(dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      doClear: Boolean)
  : Unit = {
    GraphXToGraphDB(null,dbConf,graph,isLabelCustomized = false,"","",doClear)
  }

  def GraphXToGraphDB(dbConfFilePath: String,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      vertexLabel: String,
                      edgeLabel: String)
  : Unit = {
    GraphXToGraphDB(dbConfFilePath,null,graph,isLabelCustomized = true,vertexLabel,edgeLabel,true)
  }

  def GraphXToGraphDB(dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      vertexLabel: String,
                      edgeLabel: String)
  : Unit = {
    GraphXToGraphDB(null,dbConf,graph,isLabelCustomized = false,vertexLabel,edgeLabel,true)
  }

  def GraphXToGraphDB(dbConfFilePath: String,dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      isLabelCustomized: Boolean,
                      vertexLabel: String,
                      edgeLabel: String,
                      doClear: Boolean)
  : Unit = {

    val conf = if(dbConfFilePath.isEmpty) dbConf else DataBaseUtils.loadConfFromHDFS(dbConfFilePath)

    if(doClear){
      val dbClient = createClient(conf)
      dbClient.clearGraph()
    }


    val newVertices:RDD[(VertexId, Serializable)] = graph.vertices.mapPartitions(iter => {

      val g = createClient(conf).openDB().traversal()

      iter.map(vertex => {
        var traversal = {
          if(doClear){
            val label = getLabel(vertex._2,isLabelCustomized,vertexLabel,DEFAULT_VERTEX_LABEL)
            g.addV(label)
          }else{
            val originalID = vertex._2.get("originalID")
            if(originalID.equals(null)){
              throw new Exception("GraphX vertex properties don't have originalID.")
            }else{
              g.V(originalID.asInstanceOf[Object])
            }
          }
        }
        vertex._2.asScala.foreach(attr => traversal = traversal.property(attr._1,attr._2))
        val vtemp = traversal.next()
        val vid = vtemp.id().asInstanceOf[Serializable]
        safeCommit(g)
        (vertex._1,vid)
      })
    })


    val newGraph: graphx.Graph[Serializable, util.Map[String,java.io.Serializable]]
    = {
      graph.outerJoinVertices(newVertices) { (id, oldAttr, outDegOpt) => outDegOpt.get}
    }

    newGraph.triplets.foreachPartition(itr => {

      val g = createClient(conf).openDB().traversal()

      itr.foreach(tri => {
        val src: Serializable = tri.srcAttr
        val dst: Serializable = tri.dstAttr
        var traversal = {
          if(doClear){
            // Create the edge from the src to the dst
            val label = getLabel(tri.attr,isLabelCustomized,edgeLabel,DEFAULT_EDGE_LABEL)
            g.V(src.asInstanceOf[Object]).as("a").V(dst.asInstanceOf[Object])
              .as("b").addE(label).from("a").to("b")
          }else{
            val originalID = tri.attr.get("originalID")
            if(originalID.equals(null)){
              throw new Exception("GraphX edge properties don't have originalID.")
            }else{
              g.E(originalID.asInstanceOf[Object])
            }
          }
        }

        // Add the properties of the edge
        tri.attr.asScala.foreach(attr => traversal = traversal.property(attr._1,attr._2))
        traversal.next()
      })
      safeCommit(g)
      g.getGraph.close()
    })

  }

}