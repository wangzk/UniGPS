package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters

import java.io.IOException
import java.util.{Map, Properties}
import java.{lang, util}

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.apache.tinkerpop.gremlin.structure.{Edge, T, Vertex}
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph
import CommonGraphComputer._
import cn.edu.nju.pasalab.graph.util.DBClient.client.IClient
import cn.edu.nju.pasalab.graph.util.DBClient.factory.{Neo4jClientFactory, OrientDBClientFactory}

import scala.collection.JavaConverters._

object CommonGraphX {

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

  def createDBClient(conf: Properties): IClient ={
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

}
