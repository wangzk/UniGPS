package cn.edu.nju.pasalab.graph.impl.util.DBClient.client;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Properties;

public abstract class AbstractClient implements IClient{

    private Properties clientParam;
    public Graph graph;


    @Override
    public  Properties getParam(){
        return clientParam;
    }

    @Override
    public void setClientParam(Properties clientParam) {
        this.clientParam = clientParam;
    }

    @Override
    public void safeCommit(){
        if (graph.features().graph().supportsTransactions()) graph.traversal().tx().commit();
    }

    @Override
    public void closeGraph() throws Exception {
        graph.close();
    }

    @Override
    public void clearGraph() throws Exception {
        this.graph = openDB();
        graph.traversal().V().drop().iterate();
        safeCommit();
    }
}
