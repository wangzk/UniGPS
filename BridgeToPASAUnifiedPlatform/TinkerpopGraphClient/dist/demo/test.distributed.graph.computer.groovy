hdfsGraph = GraphFactory.open('my/outg.prop')
janusGraph = 'my/janusgraph-cassandra.properties'
blvp = BulkLoaderVertexProgram.build().keepOriginalIds(false).writeGraph(janusGraph).create(hdfsGraph)
hdfsGraph.compute(SparkGraphComputer).workers(4).program(blvp).submit().get()
jgraph = GraphFactory.open(janusGraph)
jgraph.traversal().V().valueMap()
jgraph.traversal().E()
jgraph.traversal().V().drop()
jgraph.tx().commit()

ograph = GraphFactory.open('my/localdb.prop')
