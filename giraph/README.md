# GoFFish on Giraph

### Sample Command
hadoop jar goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dgiraph.metrics.enable=true in.dream_lab.goffish.giraph.GiraphSubgraphComputation -vif in.dream_lab.goffish.giraph.LongDoubleDoubleAdjacencyListSubgraphInputFormat -vip anirudh-stuff/subgraph-input-small -vof in.dream_lab.goffish.giraph.SubgraphSingleSourceShortestPathOutputFormatSir -op anirudh-stuff/output/bleh -ca giraph.subgraphVertexValueClass=org.apache.hadoop.io.DoubleWritable,subgraphSourceVertex=1,giraph.subgraphMessageValueClass=org.apache.hadoop.io.BytesWritable,giraph.outgoingMessageValueFactoryClass=in.dream_lab.goffish.giraph.DefaultSubgraphMessageFactory,giraph.messageEncodeAndStoreType=POINTER_LIST_PER_VERTEX,giraph.clientSendBufferSize=20000000,giraph.subgraphComputation=true,giraph.clientReceiveBufferSize=20000000,giraph.subgraphComputation=true,giraph.graphPartitionerFactoryClass=in.dream_lab.goffish.giraph.SubgraphPartitionerFactory,giraph.subgraphValueClass=org.apache.hadoop.io.LongWritable,giraph.vertexClass=in.dream_lab.goffish.giraph.DefaultSubgraph,subgraphComputationClass=in.dream_lab.goffish.SingleSourceShortestPathOnTemplateNoParent,giraph.edgeValueClass=org.apache.hadoop.io.DoubleWritable,giraph.vertexIdClass=in.dream_lab.goffish.giraph.SubgraphId,giraph.vertexValueClass=in.dream_lab.goffish.giraph.SubgraphVertices,giraph.outgoingMessageValueClass=in.dream_lab.goffish.giraph.SubgraphMessage -yj goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar -mc in.dream_lab.goffish.giraph.SubgraphMasterCompute -w 4 -yh 4000

### Giraph codebase changes

Apply `GiraphConfigurationValidator.patch` to the class `org.apache.giraph.job.GiraphConfigurationValidator`.

```
patch giraph/giraph-core/src/main/java/org/apache/giraph/job/GiraphConfigurationValidator.java GiraphConfigurationValidator.patch
```

