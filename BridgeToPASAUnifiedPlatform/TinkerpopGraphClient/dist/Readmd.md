# Readme

## Deploy the serial version

## Supported Operators

### 表图转换算子

Name: GopCSVFileToTinkerGraphSerial

Action: 将一个保存在HDFS上的CSV文件，转换为一个保存在TinkerPop兼容的图数据库中的图。单机串行实现。

Arguments:
  1. csvFile(String). The file path to the csv file on HDFS. The CSV file should have **a header** and at least two columns `src` and `dst`. Each line of the CSV file represents a directed simple edge from `src` to `dst`. If there is a `weight` column,  it will be interpreted as the `weight` property of the edge. The type of `weight` is double in the database. The `src` and `dst` will be treated as the values of the `name` property of the vertices in the graph.
  2. graphName(String). The name of the input graph. `GraphName` will be treated as the label attached on each vertex and edge in this CSV file.
  3. gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
  4. srcColumnName(String). The column name of the CSV file that stores the src vertex.
  5. dstColumnName(String). The column name of the CSV file that stores the dst vertex.
  6. weightColumnName(String, optional). The column name of the CSV file that stores the weights of edges. If this option is not given, it assumes that the weights of all the edges are 1.0.
  7. directed(Boolean, optional, default = true). Whether or not it is a directed graph. If `directed` = false, then two edges with `src`and `dst` are added into the graph database.
  8. overwrite (Boolean, optional, default = true). Whether or not delete the existing graph with the label `GraphName` in the database before loading the new graph in the CSV file. If `overwrite` = false and the existing graph conflicts with the new graph, exceptions will be thrown.

Output:
  1. numberOfVertices(Long).  Number of vertices in the graph.
  2. numberOfEdges(Long). Number of edges in the graph.

### 图表转换算子

Name: GopVertexPropertiesToCSVFileSerial

Action: Store the vertex properties to a CSV file on HDFS. Run in serial.

Arguments:
  1. graphName(String). The name of the input graph. It will just output the graph with the label `GraphName`.
  2. gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
  3. csvFile(String). The file path to the csv file on HDFS. The CSV file have a header with two columns `src` and `dst`. Other properties of the edge are outputed as other columns.
  4. properties(List[String]). The vertex properties to be output in the csv file.
  5. overwrite (Boolean, optional, default = true). Whether or not delete the existing CSV file on HDFS. If `overwrite` = false and `CSVFile` already exists on HDFS, an exception will be threw.

Output:
  1. numberOfLines(Long).  Number of lines in the CSV file.
  2. fileSize(Long). File size of the CSV file in bytes.

### PageRank算子

Name: GopPageRankWithGraphComputerSerial

Action: Conduct the page rank computation on a graph stored in the database. Run in serial with the graph computer on the Gremlin server.

Arguments:
  1. graphName(String). The name of the input graph. The PageRank computation will be conducted on the seletected graph with `GraphName` as the label.
  2. gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
  3. resultPropertyName (String). The property name that will store the PangRank on each vertex after the computation.
  4. returnTop(Int, optional, default=0). If `ReturnTop` is larger than 0, the vertices with the top-`ReturnTop` PageRank values will be returned in the output.

Output:
  1. elapsedTime (Long). Wall-clock elapsed time of the PageRank computation in millisecond.
  2. topVertices(Map[String, Double]). Vertices with the topest PageRank value. The key is the vertex name and the value is its PageRank.

### PeerPressure算子

Name: GopPeerPressureWithGraphComputerSerial

Action: Conduct the Peer Pressure community detection on a graph stored in the database. Run in serial with the graph computer on the Gremlin server.

Arguments:
  1. graphName(String). The name of the input graph. The Peer Pressure computation will be conducted on the seletected graph with `GraphName` as the label.
  2. gremlinServerConfFile(String). The file path to the Gremlin server remote configuration file (YAML format) on HDFS. The configuration file gives out the hosts and ports of the remote Gremlin servers. The Gremlin server should be configured and started before the operator runs.
  3. resultPropertyName (String). The property name that will store the PangRank on each vertex after the computation.

Output:
  1. elapsedTime (Long). Wall-clock elapsed time of the PageRank computation in millisecond. 
  2. numberOfClusters(Long). The number of generated clusters.