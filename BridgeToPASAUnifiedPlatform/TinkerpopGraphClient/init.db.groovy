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
mgmt = graph.openManagement();
mgmt.makeEdgeLabel("simple").multiplicity(Multiplicity.SIMPLE).make()
nameProperty = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
weightProperty = mgmt.makePropertyKey("weight").dataType(Double.class).cardinality(Cardinality.SINGLE).make();
println("create index...");
// build index on name property
mgmt.buildIndex('nameIndex', Vertex.class).addKey(nameProperty).unique().buildCompositeIndex()
mgmt.commit()
//Wait for the index to become available
ManagementSystem.awaitGraphIndexStatus(graph, 'nameIndex').call()
//Reindex the existing data
mgmt = graph.openManagement();
mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.REINDEX).get()
mgmt.commit()
graph.close();
println("done!");

