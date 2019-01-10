package cn.edu.nju.pasalab.graph.impl.DBClient.factory;

import cn.edu.nju.pasalab.graph.impl.DBClient.client.Neo4jClient;

import java.util.Properties;

public class Neo4jClientFactory extends AbstractClientFactory {
    @Override
    public Neo4jClient createClient(Properties conf) {
        Neo4jClient client = new Neo4jClient();
        client.setClientParam(conf);
        return client;
    }
}