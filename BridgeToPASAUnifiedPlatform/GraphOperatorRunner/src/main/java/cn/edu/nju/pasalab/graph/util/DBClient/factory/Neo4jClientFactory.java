package cn.edu.nju.pasalab.graph.util.DBClient.factory;

import cn.edu.nju.pasalab.graph.util.DBClient.client.Neo4jClient;

import java.util.Properties;

public class Neo4jClientFactory extends AbstractClientFactory {
    @Override
    public Neo4jClient createClient(Properties conf) {
        Neo4jClient client = new Neo4jClient();
        client.setClientParam(conf);
        return client;
    }
}
