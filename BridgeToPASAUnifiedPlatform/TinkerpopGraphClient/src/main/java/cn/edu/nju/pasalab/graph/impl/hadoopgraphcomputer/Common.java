package cn.edu.nju.pasalab.graph.impl.hadoopgraphcomputer;

import cn.edu.nju.pasalab.graph.impl.util.ConfUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Common {

    public static final String DEFAULT_VERTEX_LABEL = "SimpleV";
    public static final String DEFAULT_EDGE_LABEL = "SimpleE";
    public static final String GREMLIN_GRAPH = "gremlin.graph";
    public static final String GREMLIN_TMP_GRAPH_DIR_NAME = "~g";

    public static class ManageSparkContexts {

        public SparkContext getSc() {
            return sc;
        }

        public JavaSparkContext getJsc() {
            return jsc;
        }

        SparkContext sc;
        JavaSparkContext jsc;

        public ManageSparkContexts(String graphComputerConfFile, String appName) throws IOException, ConfigurationException {

            Configuration graphComputerConf = ConfUtils.loadConfFromHDFS(graphComputerConfFile);
            if (!graphComputerConf.getString(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER)
                    .equals(SparkGraphComputer.class.getName())) {
                // If this is not a Spark graph computer configuration file
                throw new ConfigurationException("Only SparkGraphComputer is allowed to initialize the Spark context.");
            };
            SparkConf sparkConf = new SparkConf(true);
            sparkConf.setAppName(appName);
            ConfUtils.loadUserConfToSparkConf(sparkConf, graphComputerConf);
            this.sc = new SparkContext(sparkConf);
            this.jsc = JavaSparkContext.fromSparkContext(sc);
        }

        public void stop() throws Exception {
            jsc.close();
            sc.stop();
        }

    }

    public enum VertexDirection {SRC, DST};

    public static class EdgeTuplesToVertex implements
            Function<Tuple2<String,Iterable<Tuple3<String, VertexDirection, Map<String, Object>>>>,
                    Vertex>
    {

        private class ProcessAdjEdges {
            String centerVertexName;
            Iterable<Tuple3<String, VertexDirection, Map<String, Object>>> adjs;

            public ProcessAdjEdges(String centerVertexName, Iterable<Tuple3<String, VertexDirection, Map<String, Object>>> adjs) {
                this.centerVertexName = centerVertexName;
                this.adjs = adjs;
            }

            private Vertex getOrCreate(String name) {
                if (cache.containsKey(name)) {
                    return cache.get(name);
                } else {
                    Vertex v = graph.addVertex(T.id, name, T.label, DEFAULT_VERTEX_LABEL, "name", name);
                    cache.put(name, v);
                    return v;
                }
            }

            private Edge addProperties(Edge e, Map<String, Object> properties) {
                properties.forEach((key, value) -> {
                    e.property(key.toString(), value);
                });
                return e;
            }

            StarGraph graph = StarGraph.open();
            HashMap<String, Vertex> cache = new HashMap<>();

            public Vertex constructVertex() {
                // Build center vertex
                Vertex centerVertex = getOrCreate(centerVertexName);
                // Add adjacency edges
                adjs.forEach(adjVertexTuple -> {
                    String anotherVertexName = adjVertexTuple._1();
                    VertexDirection direction = adjVertexTuple._2();
                    Map<String, Object> properties = adjVertexTuple._3();
                    Edge edge = null;
                    if (direction == VertexDirection.SRC) {
                        Vertex srcV = getOrCreate(anotherVertexName);
                        Vertex dstV = centerVertex;
                        edge = srcV.addEdge(DEFAULT_EDGE_LABEL, dstV);
                    } else if (direction == VertexDirection.DST) {
                        Vertex srcV = centerVertex;
                        Vertex dstV = getOrCreate(anotherVertexName);
                        edge = srcV.addEdge(DEFAULT_EDGE_LABEL, dstV);
                    }
                    if (edge != null && properties.size() > 0) {
                        addProperties(edge, properties);
                    }
                });
                // Return the center Vertex
                return graph.getStarVertex();
            }
        } // end of class

        /**
         * @param edgeTuples <VertexName, Iterable<<NeighborVertexName, VertexDirection, EdgeProperties>>>
         * @return Constructed center vertex.
         * @throws Exception
         */
        @Override
        public Vertex call(Tuple2<String, Iterable<Tuple3<String, VertexDirection, Map<String, Object>>>> edgeTuples) throws Exception {
            String centerVertexName = edgeTuples._1();
            ProcessAdjEdges processAdj = new ProcessAdjEdges(centerVertexName, edgeTuples._2());
            Vertex centerVertex = processAdj.constructVertex();
            return centerVertex;
        }
    }

}
