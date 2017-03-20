# GoFFish on Giraph

### Sample Command
hadoop jar goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dgiraph.metrics.enable=true in.dream_lab.goffish.giraph.graph.GiraphSubgraphComputation -vif in.dream_lab.goffish.giraph.formats.LongDoubleDoubleAdjacencyListSubgraphInputFormat -vip anirudh-stuff/subgraph-input-small -vof in.dream_lab.goffish.giraph.formats.SubgraphSingleSourceShortestPathOutputFormatSir -op anirudh-stuff/output/blehfinal -ca giraph.subgraphVertexValueClass=org.apache.hadoop.io.DoubleWritable,subgraphSourceVertex=1,giraph.subgraphMessageValueClass=org.apache.hadoop.io.BytesWritable,giraph.outgoingMessageValueFactoryClass=in.dream_lab.goffish.giraph.factories.DefaultSubgraphMessageFactory,giraph.messageEncodeAndStoreType=POINTER_LIST_PER_VERTEX,giraph.clientSendBufferSize=20000000,giraph.clientReceiveBufferSize=20000000,giraph.graphPartitionerFactoryClass=in.dream_lab.goffish.giraph.factories.SubgraphPartitionerFactory,giraph.subgraphValueClass=org.apache.hadoop.io.LongWritable,giraph.vertexClass=in.dream_lab.goffish.giraph.graph.DefaultSubgraph,subgraphComputationClass=in.dream_lab.goffish.giraph.examples.SingleSourceShortestPath,giraph.edgeValueClass=org.apache.hadoop.io.DoubleWritable,giraph.vertexIdClass=in.dream_lab.goffish.giraph.graph.SubgraphId,giraph.vertexValueClass=in.dream_lab.goffish.giraph.graph.SubgraphVertices,giraph.outgoingMessageValueClass=in.dream_lab.goffish.giraph.graph.SubgraphMessage -yj goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar -mc in.dream_lab.goffish.giraph.master.SubgraphMasterCompute -w 4 -yh 4000

### Giraph codebase changes

Apply `GiraphConfigurationValidator.patch` to the class `org.apache.giraph.job.GiraphConfigurationValidator`.

```
patch giraph/giraph-core/src/main/java/org/apache/giraph/job/GiraphConfigurationValidator.java GiraphConfigurationValidator.patch
```

