package cn.edu.nju.pasalab.graph.util.DBClient.factory;
import cn.edu.nju.pasalab.graph.util.DBClient.client.IClient;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class AbstractClientFactory implements IClientFactory{

    @Override
    public IClient createClient(String confPath) throws IOException {
        Properties conf = new Properties();
        InputStream fileStream = HDFSUtils.openFile(confPath);
        conf.load(fileStream);
        fileStream.close();
        return createClient(conf);
    }

}
