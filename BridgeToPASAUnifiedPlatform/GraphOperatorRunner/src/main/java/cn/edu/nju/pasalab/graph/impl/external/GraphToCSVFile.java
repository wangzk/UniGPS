package cn.edu.nju.pasalab.graph.impl.external;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv.GraphSONToGraphCSVGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographcsv.graphsontographcsv.GraphSONToGraphCSVSerial;

import java.util.Map;

public class GraphToCSVFile {

    public static void fromGraphSON(Map<String, String> arguments) throws Exception {
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_SERIAL)) {
            GraphSONToGraphCSVSerial.converter(arguments);
        } else if (runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            GraphSONToGraphCSVGraphX.converter(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }
}
