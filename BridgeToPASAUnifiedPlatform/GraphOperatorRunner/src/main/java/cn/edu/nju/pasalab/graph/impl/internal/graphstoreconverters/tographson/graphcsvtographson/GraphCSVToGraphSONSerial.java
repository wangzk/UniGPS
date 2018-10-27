package cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson;

import java.util.Map;

public class GraphCSVToGraphSONSerial {

    public static void converter(Map<String, String> arguments) throws Exception {
        // the parameter graphComputerConfFile in the arguments is null
        GraphCSVToGraphSONGraphX.converter(arguments);
    }
}
