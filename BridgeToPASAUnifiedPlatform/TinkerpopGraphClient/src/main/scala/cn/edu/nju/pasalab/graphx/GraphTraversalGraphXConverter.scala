package cn.edu.nju.pasalab.graphx

import java.{lang, util}

import cn.edu.nju.pasalab.graphx.GraphSONGraphXConverter.convertStringIDToLongID
import org.apache.spark.{SparkContext, graphx}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource


object GraphTraversalGraphXConverter {

  val DEFAULT_VERTEX_LABEL = "SimpleV"
  val DEFAULT_EDGE_LABEL = "SimpleE"

  def getOrCreateVertexID(g:GraphTraversalSource,
                        cache:util.HashMap[lang.Long, lang.Long],
                        name: lang.Long,
                        properties :util.HashMap[String, java.io.Serializable]):lang.Long = {

    if (cache.containsKey(name)) cache.get(name)
    else{
      // Get the vertexID then use the ID to add new properties
      val vid :lang.Long = g.addV(DEFAULT_VERTEX_LABEL).id().next().toString.toLong
      val v = g.V(vid)
      for(key <- properties.keySet().toArray){
        v.property(key.toString,properties.get(key).toString)
      }
      v.iterate()

      cache.put(name, vid)
      vid
    }
  }
  def GraphTraversalToGraphX(g : GraphTraversalSource,sc : SparkContext)
  : graphx.Graph[util.HashMap[String, java.io.Serializable],
                  util.HashMap[String, java.io.Serializable]] ={

    // Store the edges for the GraphX
    val graphxEdge = new collection.mutable.ArrayBuffer[graphx.Edge[util.HashMap[String,java.io.Serializable]]]()

    // The value edgelist doesn't contain the properties of the graph,
    // so we need to use the valueMap step
    val edgelist = g.E().toList
    val edgeAttrlist = g.E().valueMap().toList
    var x = 0
    for(x <- 0 until edgelist.size){
      var srcId = edgelist.get(x).inVertex.id().toString
      var dstId = edgelist.get(x).outVertex.id().toString
      val md1 = convertStringIDToLongID(srcId)
      val md2 = convertStringIDToLongID(dstId)
      // Add the properties of the edge
      var edgeAttr = new util.HashMap[String,java.io.Serializable]()
      for(key <- edgeAttrlist.get(x).keySet().toArray){
        edgeAttr.put(key.toString,edgeAttrlist.get(x).get(key).toString)
      }
      graphxEdge.append(graphx.Edge(md1,md2,edgeAttr))
    }

    // Store the vertices for the GraphX
    val graphxVertex = new collection.mutable.ArrayBuffer[(Long, util.HashMap[String,java.io.Serializable])]()

    // Same process as the edges above
    val vertexlist = g.V().toList
    val vertexAttrlist = g.V().valueMap().toList
    x = 0
    for(x <- 0 until vertexlist.size()){
      val vertexId = vertexlist.get(x).id().toString
      val md = convertStringIDToLongID(vertexId)
      var vertexAttr : util.HashMap[String,java.io.Serializable] = new util.HashMap[String,java.io.Serializable]()
      vertexAttr.put("originalID",vertexId)
      for(key <- vertexAttrlist.get(x).keySet().toArray){
        vertexAttr.put(key.toString,vertexAttrlist.get(x).get(key).toString)
      }
      graphxVertex.append((md,vertexAttr))
    }

    // Make the RDD for GraphX
    val vertexRDD = sc.parallelize(graphxVertex)
    val edgeRDD = sc.parallelize(graphxEdge)

    // Appoint the type of the vertex and edge attributes
    graphx.Graph[util.HashMap[String,java.io.Serializable],
      util.HashMap[String,java.io.Serializable]](vertexRDD,edgeRDD,new util.HashMap[String,java.io.Serializable]())
  }

  def GraphXWriteToTraversal(g : GraphTraversalSource,
                             graph : graphx.Graph[util.HashMap[String, java.io.Serializable],
                               util.HashMap[String, java.io.Serializable]])
  : Unit = {

    // Clear the graph that already exists
    g.V().drop().iterate()

    // Cache the vertexIDs created
    val cache = new util.HashMap[lang.Long, lang.Long]

    graph.triplets.collect().foreach(tri => {
      val srcID = getOrCreateVertexID(g,cache,tri.srcId,tri.srcAttr)
      val dstID = getOrCreateVertexID(g,cache,tri.dstId,tri.dstAttr)

      // Create the edge from the src to the dst
      val edge = g.V(srcID).as("s").V(dstID).as("d")
        .addE(DEFAULT_EDGE_LABEL).from("s").to("d")
      // Add the properties of the edge
      for(key <- tri.attr.keySet().toArray){
        edge.property(key.toString,tri.attr.get(key).toString)
      }
      edge.iterate()
    })
  }
}
