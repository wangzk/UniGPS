package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb

import java.io.Serializable
import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer.GREMLIN_GRAPH
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.GraphDBGraphXConverter.createClient
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.{CommonGraphComputer, CommonGraphX, CommonSerial}
import org.apache.commons.configuration.BaseConfiguration
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.tinkerpop.gremlin.hadoop.Constants
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD
import org.apache.tinkerpop.gremlin.structure.{Direction, Property, T, VertexProperty}
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object GraphSONToGraphDBGraphX {
  def converter(arguments: util.Map[String, String]):Unit = {
    //////////// Arguments
    // For GraphSON file, the conf path is the graph file path
    val inputGraphFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_INPUT_GRAPH_CONF_FILE)
    val graphComputerConfFile = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_RUNMODE_CONF_FILE)
    val dbConfFilePath = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_CONF_FILE)
    val outputGraphType = arguments.get(cn.edu.nju.pasalab.graph.Constants.ARG_OUTPUT_GRAPH_TYPE)

    ////////// Input graph
    val inputGraphConf = new BaseConfiguration
    inputGraphConf.setProperty(GREMLIN_GRAPH, classOf[HadoopGraph].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, classOf[GraphSONInputFormat].getName)
    inputGraphConf.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputGraphFilePath)
    inputGraphConf.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputGraphFilePath)

    ////////// Output vertex data
    val manageSparkContexts = new CommonGraphComputer.ManageSparkContexts(graphComputerConfFile, "GraphSON File to GraphDB")
    val sc = manageSparkContexts.getSc
    val jsc = manageSparkContexts.getJsc
    val graphRDDInput = new InputFormatRDD
    val vertexWritableJavaPairRDD = graphRDDInput.readGraphRDD(inputGraphConf, jsc)
    val vidCache = new util.HashMap[String, Object]

    val dbClient = CommonSerial.createGraphDBCient(dbConfFilePath, outputGraphType)
    dbClient.clearGraph()

    /////////// Store the map of old vertexID and graphdb new vertexID
    val newVertices:RDD[(String,Object)] = vertexWritableJavaPairRDD.rdd.mapPartitions(tuple2Iterator => {
      val graphdb = CommonSerial.createGraph(dbConfFilePath, outputGraphType)
      val gdb = graphdb.traversal
      tuple2Iterator.map(tuple => {
        val v = tuple._2.get()
        //v.vertices
        //v.edges
        val g = StarGraph.of(v)
        var traversal = gdb.addV(v.label)
        //traversal.property("originalID", v.id)
        v.properties().asScala.foreach((vp : VertexProperty[Nothing]) => {
          traversal = traversal.property(vp.key(),vp.value())
        })
        val vtmp = traversal.next()
        CommonSerial.safeCommit(gdb)
        val vid = vtmp.id()
        (v.id().toString,vid)
      })
    })

    /////// Old edge with old src and dst vertexID
    val edgelistRDD = vertexWritableJavaPairRDD.rdd.flatMap(tuple => {
      val v = tuple._2.get()
      val g = StarGraph.of(v)
      g.traversal.V(v.id).outE()
    })

    newVertices.cache()

    val edgeWithInVertexIDRDD = edgelistRDD.map(edge => {
      // Get the properties of the edge
      var edgeAttr = new util.HashMap[String,java.io.Serializable]()
      edge.properties().asScala.foreach((pro:Property[Nothing])=>
      {edgeAttr.put(pro.key(),pro.value().toString)})
      edgeAttr.put("label",edge.label())
      (edge.inVertex().id().toString,edgeAttr)
    })
    val edgeWithOutVertexIDRDD = edgelistRDD.map(edge => {
      var edgeAttr = new util.HashMap[String,java.io.Serializable]()
      edge.properties().asScala.foreach((pro:Property[Nothing])=>
      {edgeAttr.put(pro.key(),pro.value().toString)})
      edgeAttr.put("label",edge.label())
      (edge.outVertex().id().toString,edgeAttr)
    })
    val edgeAsKeyInVertexRDD: RDD[(util.HashMap[String, Serializable], (String, Object))] = edgeWithInVertexIDRDD.join(newVertices).map(triple => {
      (triple._2._1,(triple._1,triple._2._2))
    })
    val edgeAsKeyOutVertexRDD: RDD[(util.HashMap[String, Serializable], (String, Object))] = edgeWithOutVertexIDRDD.join(newVertices).map(triple => {
      (triple._2._1,(triple._1,triple._2._2))
    })
    val tripleRDD = edgeAsKeyInVertexRDD.join(edgeAsKeyOutVertexRDD).map(tri => {
      (tri._2._1._2,tri._2._2._2,tri._1)
    })

    tripleRDD.foreachPartition(itr => {
      val graphdb = CommonSerial.createGraph(dbConfFilePath, outputGraphType)
      val gdb = graphdb.traversal
      itr.foreach(tri => {
        val srcID = tri._1
        val dstID = tri._2
        val edgePro = tri._3
        var traversal = gdb.V(srcID).as("a").V(dstID).as("b")
          .addE(edgePro.get("label").toString).from("a").to("b")
        //traversal.property("originalID",edgePro.id)
        edgePro.asScala.foreach(ep  => {
          traversal = traversal.property(ep._1,ep._2)
        })
        traversal.next()
        CommonSerial.safeCommit(gdb)
      })

    })
    sc.stop()

    /*val srcOriginalID = v.id().toString
      val srcVertex = CommonGraphX.getOrCreateVertexForGraphDB(g,vidCache,srcOriginalID,true,gdb)
      v.edges(Direction.OUT).asScala.foreach(edge => {
      val dstOriginalID = edge.inVertex().id().toString
      val dstVertex = CommonGraphX.getOrCreateVertexForGraphDB(g,vidCache,dstOriginalID,false,gdb)
      var traversal = gdb.V(srcVertex.id()).as("a").V(dstVertex.id()).as("b")
        .addE(edge.label).from("a").to("b")
      traversal.property("originalID",edge.id)
      edge.properties().asScala.foreach((ep : Property[Nothing])  => {
        traversal = traversal.property(ep.key(),ep.value())
      })
      traversal.next()
    })*/

    /*  var traversal = gdb.addV(v.label)
        traversal.property("originalID", v.id)
        v.properties().asScala.foreach((vp : VertexProperty[Nothing]) => {
          traversal = traversal.property(vp.key(),vp.value())
        })
        //val vtmp = traversal.next()
        CommonSerial.safeCommit(gdb)
        //println(vtmp.id())*/
    ////////// Handle edge data
    /*val edgelistRDD = vertexWritableJavaPairRDD.rdd.flatMap(tuple => {
      val v = tuple._2.get()
      val g = StarGraph.of(v)
      g.traversal.V(v.id).outE()
    })

    edgelistRDD.foreachPartition(iter => {
      val graphdb = CommonSerial.createGraph(dbConfFilePath, outputGraphType)
      val gdb = graphdb.traversal
      iter.foreach(edge => {
        var traversal = gdb.V(edge.inVertex).as("a").V(edge.outVertex).as("b")
          .addE(edge.label).from("a").to("b")
        traversal.property("originalID",edge.id)
        edge.properties().asScala.foreach((ep : Property[Nothing])  => {
          traversal = traversal.property(ep.key(),ep.value())
        })
      })
      CommonSerial.safeCommit(gdb)
    })*/

    //System.out.println("Finished load the GraphSON to GraphDB, the total edge count is " + edgelistRDD.count)


  }
}
