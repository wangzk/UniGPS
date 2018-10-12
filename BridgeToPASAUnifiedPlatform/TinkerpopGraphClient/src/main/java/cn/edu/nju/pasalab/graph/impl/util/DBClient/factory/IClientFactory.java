package cn.edu.nju.pasalab.graph.impl.util.DBClient.factory;

import cn.edu.nju.pasalab.graph.impl.util.DBClient.client.IClient;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public interface IClientFactory extends Serializable {

    IClient createClient(String confPath) throws IOException;

    IClient createClient(Properties conf) throws IOException;

}
