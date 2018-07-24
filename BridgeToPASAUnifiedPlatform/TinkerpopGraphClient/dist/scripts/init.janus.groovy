import org.janusgraph.core.Cardinality
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.Multiplicity

dbFile = args[0]
println("==================== DB INIT SCRIPT by wzk ===============");
println("Init database configured in " + dbFile + "...");
println("drop old database...");
oldgraph = JanusGraphFactory.open(dbFile);
JanusGraphFactory.drop(oldgraph);
oldgraph.close();
println("create new database...");
graph = JanusGraphFactory.open(dbFile);
graph.tx().rollback();
mgmt = graph.openManagement();
nameProperty = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
weightProperty = mgmt.makePropertyKey("weight").dataType(Double.class).cardinality(Cardinality.SINGLE).make();
mgmt.commit()

// build index on name property
println("create index...");
graph.tx().rollback()
mgmt = graph.openManagement();
nameIdx = mgmt.buildIndex("byNameIndex", Vertex.class)
nameProp = mgmt.getPropertyKey("name")
nameIdx.addKey(nameProp).buildCompositeIndex()
mgmt.commit()

//Wait for the index to become available
mgmt.awaitGraphIndexStatus(graph, 'byNameIndex').status(SchemaStatus.REGISTERED).call()

mgmt = graph.openManagement()
mgmt.awaitGraphIndexStatus(graph, 'byNameIndex').call()
mgmt.getGraphIndex('byNameIndex')
mgmt.updateIndex(mgmt.getGraphIndex('byNameIndex'), SchemaAction.REINDEX).get()
mgmt.commit()

// Display
mgmt = graph.openManagement()
v_idxes = mgmt.getGraphIndexes(Vertex.class)
println v_idxes
v_idxes.each {println it}
mgmt.commit()


println("done!");

