package cn.edu.nju.pasalab.graph.util.DBClient.client;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Properties;

public interface IClient extends java.io.Serializable{

    Graph openDB() throws Exception;

    void clearGraph() throws Exception;

    void closeGraph() throws Exception;

    void safeCommit();

    void setClientParam(Properties clientParam);

    Properties getParam();
}
