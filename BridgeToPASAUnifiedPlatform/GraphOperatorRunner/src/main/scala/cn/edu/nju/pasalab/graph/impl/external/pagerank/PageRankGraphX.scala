package cn.edu.nju.pasalab.graph.impl.external.pagerank

import java.util
import java.util.HashMap

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonGraphComputer
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographrdd.GraphSONToGraphRDD
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.GraphRDDToGraphSON
import cn.edu.nju.pasalab.graph.util.HDFSUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx
import org.apache.spark.graphx.lib.LabelPropagation

object PageRankGraphX {
  def fromGraphRDDToGraphRDD(arguments: util.Map[String, String]): Unit = {

  }
}
