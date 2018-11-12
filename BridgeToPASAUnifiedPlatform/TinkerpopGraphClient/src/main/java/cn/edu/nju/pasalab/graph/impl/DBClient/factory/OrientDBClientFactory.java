package cn.edu.nju.pasalab.graph.impl.DBClient.factory;

import cn.edu.nju.pasalab.graph.impl.DBClient.client.OrientDBClient;

import java.util.Properties;

public class OrientDBClientFactory extends AbstractClientFactory {
    @Override
    public OrientDBClient createClient(Properties conf) {
        OrientDBClient client = new OrientDBClient();
        client.setClientParam(conf);
        return client;
    }
}
