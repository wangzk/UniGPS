package cn.edu.nju.pasalab.graph.impl.external;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson.GraphCSVToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson.GraphCSVToGraphSONSerial;

import java.util.Map;

public class CSVFileToGraph {

    public static void toGraphSON(Map<String, String> arguments) throws Exception {
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_SERIAL)) {
            GraphCSVToGraphSONSerial.converter(arguments);
        } else if (runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            GraphCSVToGraphSONGraphX.converter(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }

}
