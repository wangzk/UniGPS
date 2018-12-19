package cn.edu.nju.pasalab.graph.demo;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.*;
import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.GraphOperators;

import java.util.HashMap;
import java.util.Map;

public class DistributedGraphOp {

    private String inputEdgeCSVFile;
    private String inputVertexCSVFile;
    private String graphComputerConfFile;

    public DistributedGraphOp(String inputEdgeCSVFile, String inputVertexCSVFile, String graphComputerConfFile) {
        this.inputEdgeCSVFile = inputEdgeCSVFile;
        this.inputVertexCSVFile = inputVertexCSVFile;
        this.graphComputerConfFile = graphComputerConfFile;
    }

    public void testGopCSVFileToGraphDB() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopCSVFileToGraph(inputEdgeCSVFile, "p1","p2",
                "weight","true",Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/Neo4j.conf",Constants.RUNMODE_SPARK_GRAPHX,
                "/home/lijunhong/graphxtosontest/SparkLocal.conf",
                inputVertexCSVFile,"name","test");
    }

    public void testGopCSVFileToGraphSON() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopCSVFileToGraph(inputEdgeCSVFile, "p1","p2",
                "weight","true",Constants.GRAPHTYPE_GRAPHSON,
                inputVertexCSVFile+"/graphson",Constants.RUNMODE_SPARK_GRAPHX,
                "/home/lijunhong/graphxtosontest/SparkLocal.conf",
                inputVertexCSVFile,"name","test");
    }

    public void testGopLabelPropagation() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopLabelPropagation(Constants.GRAPHTYPE_GRAPHSON, inputEdgeCSVFile + ".graph",
                "clusterID",Constants.RUNMODE_TINKERPOP_GRAPHCOMPUTER,
                graphComputerConfFile,Constants.GRAPHTYPE_GRAPHSON,
                inputEdgeCSVFile + ".aftergraphxlp");
    }

    public void testDBGopLabelPropagation() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopLabelPropagation(Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/Neo4j.conf",
                "clusterID",Constants.RUNMODE_TINKERPOP_GRAPHCOMPUTER,
                graphComputerConfFile,Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/Neo4j.conf");
    }

    public void testGopGraphToCSVFile() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopGraphToCSVFile(Constants.GRAPHTYPE_GRAPHSON,
                inputVertexCSVFile + "/graphsonafterlp",
                "test",inputVertexCSVFile + ".out",
                "weight",inputEdgeCSVFile + ".out",
                Constants.RUNMODE_SPARK_GRAPHX,graphComputerConfFile
                );
    }

    public void testGopGraphDBToCSVFile() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopGraphToCSVFile(Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/Neo4j.conf",
                "clusterID",inputVertexCSVFile + ".out",
                "weight",inputEdgeCSVFile + ".out",
                Constants.RUNMODE_SPARK_GRAPHX,graphComputerConfFile
        );
    }

    public void testGopGraphDBToSecCSVFile() throws Exception {
        GraphOperators op = new GraphOperators();
        op.GopGraphToCSVFile(Constants.GRAPHTYPE_GRAPHDB_NEO4J,
                "/home/lijunhong/graphxtosontest/Neo4j.conf",
                "name",inputVertexCSVFile + ".secout",
                "weight",inputEdgeCSVFile + ".secout",
                Constants.RUNMODE_SPARK_GRAPHX,graphComputerConfFile
        );
    }

    public void run() throws Exception {
        testGopCSVFileToGraphDB();
        //testGopLabelPropagation();
        testDBGopLabelPropagation();
        //testGopGraphToCSVFile();
        testGopGraphDBToCSVFile();
        //testGopGraphDBToSecCSVFile();
    }

    public static void main(String args[]) throws Exception {
        String inputVertexCSVFile = "/home/lijunhong/graphxtosontest/vertex.csv";
        //String inputVertexCSVFile = null;
        String inputfile = args[0];
        //String inputVertexCSVFile = inputfile + "/vertex";
        String inputEdgeCSVFile = "/home/lijunhong/graphxtosontest/test.csv";
        //String inputEdgeCSVFile = inputfile + "/test";
        String graphComputerPath = inputfile + "/SparkLocal.conf";
        DistributedGraphOp op = new DistributedGraphOp(inputEdgeCSVFile, inputVertexCSVFile,graphComputerPath);
        op.run();
    }
}
