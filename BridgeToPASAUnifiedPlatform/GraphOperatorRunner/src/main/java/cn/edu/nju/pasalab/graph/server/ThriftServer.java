package cn.edu.nju.pasalab.graph.server;

import cn.edu.nju.pasalab.graph.GraphOperators;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

public class ThriftServer {
    public static final int SERVER_PORT = 8090;

    public void startServer() {
        try {
            System.out.println("Thrift Server start ....");

            TProcessor tprocessor = new GraphOperatorsService.Processor<GraphOperatorsService.Iface>(new GraphOperators());

            // 简单的单线程服务模型，用于测试
            TServerSocket serverTransport = new TServerSocket(SERVER_PORT);
            TServer.Args tArgs = new TServer.Args(serverTransport);
            tArgs.processor(tprocessor);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TSimpleServer(tArgs);
            server.serve();

        } catch (Exception e) {
            System.out.println("Server start error!!!");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        ThriftServer server = new ThriftServer();
        server.startServer();
    }
}
