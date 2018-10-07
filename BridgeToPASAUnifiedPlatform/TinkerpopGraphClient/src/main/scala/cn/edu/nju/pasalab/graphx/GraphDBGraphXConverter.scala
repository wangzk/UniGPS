package cn.edu.nju.pasalab.graphx

import java.io.Serializable
import java.util.{Properties, Scanner}
import java.{lang, util}

import cn.edu.nju.pasalab.graph.impl.util.DataBaseUtils
import cn.edu.nju.pasalab.graphx.GraphSONGraphXConverter.convertStringIDToLongID

import org.apache.spark.{SparkContext, graphx}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Property, VertexProperty}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


object GraphDBGraphXConverter extends Serializable{

  val DEFAULT_VERTEX_LABEL = "SimpleV"
  val DEFAULT_EDGE_LABEL = "SimpleE"

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
                        util.Map[String, java.io.Serializable]])
  : Unit = {
    GraphXToGraphDB(dbConfFilePath,null,graph,isLabelCustomized = false,"","")
  }

  def GraphXToGraphDB(dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]])
  : Unit = {
    GraphXToGraphDB(null,dbConf,graph,isLabelCustomized = false,"","")
  }

  def GraphXToGraphDB(dbConfFilePath: String,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      vertexLabel: String,
                      edgeLabel: String)
  : Unit = {
    GraphXToGraphDB(dbConfFilePath,null,graph,isLabelCustomized = true,vertexLabel,edgeLabel)
  }

  def GraphXToGraphDB(dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                        util.Map[String, java.io.Serializable]],
                      vertexLabel: String,
                      edgeLabel: String)
  : Unit = {
    GraphXToGraphDB(null,dbConf,graph,isLabelCustomized = false,vertexLabel,edgeLabel)
  }

  def GraphXToGraphDB(dbConfFilePath: String,dbConf: Properties,
                      graph: graphx.Graph[util.Map[String, java.io.Serializable],
                               util.Map[String, java.io.Serializable]],
                      isLabelCustomized: Boolean,
                      vertexLabel: String,
                      edgeLabel: String)
  : Unit = {

    val conf = if(dbConfFilePath.isEmpty) dbConf else DataBaseUtils.loadConfFromHDFS(dbConfFilePath)
    // Clear the graph that already exists
    val g = DataBaseUtils.openDB(conf).traversal()
    g.V().drop().iterate()
    safeCommit(g)


    val newGraph: graphx.Graph[lang.Long,util.Map[String,java.io.Serializable]] =
      graph.mapVertices { case (id, properties) =>
        val graph = DataBaseUtils.openDB(conf)
        val g = graph.traversal()
        val label = getLabel(properties,isLabelCustomized,vertexLabel,DEFAULT_VERTEX_LABEL)

        var traversal = g.addV(label)
        properties.asScala.foreach(attr => traversal = traversal.property(attr._1,attr._2))
        val vid = traversal.next()
        safeCommit(g)
        vid.id().asInstanceOf[lang.Long]
      }

    newGraph.triplets.foreachPartition(itr => {

      val graph = DataBaseUtils.openDB(conf)
      val g = graph.traversal()
      itr.foreach(tri => {

        val src:lang.Long = tri.srcAttr
        val dst:lang.Long = tri.dstAttr

        val label = getLabel(tri.attr,isLabelCustomized,edgeLabel,DEFAULT_EDGE_LABEL)

        // Create the edge from the src to the dst
        var traversal = g.V(src).as("a").V(dst).as("b").addE(label)
          .from("a").to("b")

        // Add the properties of the edge
        tri.attr.asScala.foreach(attr => traversal = traversal.property(attr._1,attr._2))

        traversal.next()
      })
      safeCommit(g)
    })
    var scan=new Scanner(System.in)
    scan.next()
    println("*****************************")

  }

}
