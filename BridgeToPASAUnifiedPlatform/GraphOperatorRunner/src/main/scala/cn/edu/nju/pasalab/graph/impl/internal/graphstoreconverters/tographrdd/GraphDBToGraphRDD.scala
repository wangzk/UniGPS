package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd

import java.io.PrintWriter
import java.{io, util}

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphX.convertStringIDToLongID
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphX.createDBClient
import cn.edu.nju.pasalab.graph.util.{DataBaseUtils, HDFSUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, graphx}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GraphDBToGraphRDD {

  def converter(g: GraphTraversalSource, sc: SparkContext,
                dbConfFilePath: String)
  : Graph[util.Map[String, io.Serializable], util.Map[String, io.Serializable]] = {

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

    val vertexRDD = vertexlist.mapPartitions(vertexIter => {
      val g = createDBClient(conf).openDB().traversal()
      val res = ArrayBuffer[(Long,util.Map[String,java.io.Serializable])]()
      while (vertexIter.hasNext){

        val vertexId = vertexIter.next()
        val vertex = g.V(vertexId).next()
        val md = convertStringIDToLongID(vertexId)
        val vertexAttr: util.Map[String,java.io.Serializable] = new util.HashMap[String,java.io.Serializable]()
        vertexAttr.put("label",vertex.label())
        vertexAttr.put("originalID",vertexId)
        val attr: java.util.Iterator[VertexProperty[Nothing]] = vertex.properties()
        attr.foreach(itr => {
          vertexAttr.put(itr.key(),itr.value().toString)
        })
        res.append((md,vertexAttr))
      }
      res.iterator
    })

    val edgeRDD = vertexlist.mapPartitions(vertexIter => {
      val g = createDBClient(conf).openDB().traversal()
      val res = new ArrayBuffer[Edge[util.Map[String,java.io.Serializable]]]()
      while (vertexIter.hasNext){
        val vertexId = vertexIter.next()
        val edgeItr = g.V(vertexId).outE()
        val md = convertStringIDToLongID(vertexId)
        while(edgeItr.hasNext){
          val edge = edgeItr.next()
          val srcId = edge.inVertex.id().toString
          val md1 = convertStringIDToLongID(srcId)
          // Add the properties of the edge
          val edgeAttr: util.Map[String,java.io.Serializable] = new util.HashMap[String,java.io.Serializable]()
          edgeAttr.put("label",edge.label())
          edgeAttr.put("originalID",edge.id().toString)
          val attr: util.Iterator[Property[Nothing]] = edge.properties()
          attr.foreach(itr => {
            edgeAttr.put(itr.key(),itr.value().toString)
          })
          res.append(graphx.Edge(md, md1, edgeAttr))
        }
      }
      res.iterator
    })

    graphx.Graph[util.Map[String,java.io.Serializable],
      util.Map[String,java.io.Serializable]](vertexRDD,edgeRDD,new util.HashMap[String,java.io.Serializable]())

  }
}
