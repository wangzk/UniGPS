package cn.edu.nju.pasalab.graph.demo.client;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.server.GraphOperatorsService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

public class ThriftClient {
    public static final String SERVER_IP = "localhost";
    public static final int SERVER_PORT = 8090;
    public static final int TIMEOUT = 30000;


    public void startClient(Map<String, String> arguments) {
        TTransport transport = null;
        try {
            transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            // TProtocol protocol = new TCompactProtocol(transport);
            // TProtocol protocol = new TJSONProtocol(transport);
            GraphOperatorsService.Client client = new GraphOperatorsService.Client(
                    protocol);
            transport.open();
            client.GopCSVFileToGraph(arguments);
            System.out.println("Thrift success!");
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ThriftClient client = new ThriftClient();
        Map<String, String> arguments = new HashMap<>();
        String inputCSVFile = "/home/lijunhong/graphxtosontest/test.csv";
        String graphComputerConfFile = "/home/lijunhong/IdeaProjects/GraphOperator/BridgeToPASAUnifiedPlatform/TinkerpopGraphClient/conf/graph-computer/SparkLocal.conf";

        arguments.put(Constants.ARG_INPUT_EDGE_CSV_FILE_PATH, inputCSVFile);
        arguments.put(Constants.ARG_EDGE_SRC_COLUMN, "p1");
        arguments.put(Constants.ARG_EDGE_DST_COLUMN, "p2");
        arguments.put(Constants.ARG_DIRECTED, "true");
        arguments.put(Constants.ARG_OUTPUT_GRAPH_TYPE, Constants.GRAPHTYPE_GRAPHSON);
        arguments.put(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, inputCSVFile + ".graph");
        arguments.put(Constants.ARG_RUNMODE, Constants.RUNMODE_SPARK_GRAPHX);
        arguments.put(Constants.ARG_RUNMODE_CONF_FILE, graphComputerConfFile);
        arguments.put(Constants.ARG_EDGE_PROPERTY_COLUMNS, "weight");
        client.startClient(arguments);
    }
}
