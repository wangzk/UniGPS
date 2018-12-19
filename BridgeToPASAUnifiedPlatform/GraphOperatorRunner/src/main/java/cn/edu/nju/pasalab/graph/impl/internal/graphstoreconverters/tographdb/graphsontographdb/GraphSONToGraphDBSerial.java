package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.graphsontographdb;

import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.CommonSerial;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;

public class GraphSONToGraphDBSerial {
    public static void converter(Map<String, String> arguments) throws Exception {

        Graph graph = CommonSerial.loadGraphSONToTinkerGraph(arguments);

    }
}
