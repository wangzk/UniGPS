package cn.edu.nju.pasalab.graph.impl.external;

import cn.edu.nju.pasalab.graph.Constants;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographdb.graphsontographdb.GraphSONToGraphDBSerial;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson.GraphCSVToGraphSONGraphX;
import cn.edu.nju.pasalab.graph.impl.internal.graphstoreconverters.tographson.graphcsvtographson.GraphCSVToGraphSONSerial;
import cn.edu.nju.pasalab.graph.util.DataBaseUtils;
import cn.edu.nju.pasalab.graph.util.HDFSUtils;

import java.util.Map;
import java.util.Properties;

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

    public static void toGraphDB(Map<String, String> arguments) throws Exception {
        String runMode = (String)arguments.get(Constants.ARG_RUNMODE);
        /////// Determine run mode.
        if (runMode.equals(Constants.RUNMODE_SERIAL)) {
            String dbConfFilePath = arguments.get(Constants.ARG_OUTPUT_GRAPH_CONF_FILE);
            ///create tmp file path for the GraphSON
            Properties conf = DataBaseUtils.loadConfFromHDFS(dbConfFilePath);
            String tmpDirPath = conf.getProperty("tmpdirpath");
            String tmpFilePath = tmpDirPath + HDFSUtils.getTimeName();
            arguments.replace(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, tmpFilePath);
            GraphCSVToGraphSONSerial.converter(arguments);

            arguments.replace(Constants.ARG_OUTPUT_GRAPH_CONF_FILE, dbConfFilePath);
            arguments.put(Constants.ARG_INPUT_GRAPH_CONF_FILE, tmpFilePath);
            GraphSONToGraphDBSerial.converter(arguments);

        } else if (runMode.equals(Constants.RUNMODE_SPARK_GRAPHX)) {
            GraphCSVToGraphSONGraphX.converter(arguments);
        } else {
            throw new UnsupportedOperationException("No implementation for run mode:" + runMode);
        }

    }

}
