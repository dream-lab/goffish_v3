#GoFFish on Hama

Follow the steps to install GoFFish

1. Install Hadoop and Hama using the this [link](http://people.apache.org/~tjungblut/downloads/hamadocs/ApacheHamaInstallationGuide_06.pdf).
2. Clone and build using the following commands:

    ```
    git clone https://github.com/dream-lab/goffish_v3
    cd goffish_v3/hama/v0.1/
    mvn clean install
    ```

3. Graphs

   3.1 [Generate random graph using hama-examples](https://hama.apache.org/run_examples.html):

   ```
   $HAMA_HOME/bin/hama jar hama-examples-x.x.x.jar gen fastgen -v 100 -e 10 -o randomgraph -t 2
   ```

   3.2 Supported graph formats

   3.2.1 Adjacency List (LongTextAdjacencyListReader.java)
   *srcId sinkVertexId sinkVertexId*

   ```
   job.setInputFormat(NonSplitTextInputFormat.class);
   job.setInputReaderClass(LongTextAdjacencyListReader.class);
   ```

   If no reader is specified in the job, this is used as default reader.

   3.2.2 Partitioned Adjacency List ( PartitionsLongTextAdjacencyReader.java)
   *srcId partitionID sinkVertexId sinkVertexId*

   ```
   job.setInputFormat(TextInputFormat.class);
   job.setInputReaderClass(PartitionsLongTextAdjacencyReader.class);
   ```
    3.2.3 JSON Reader (LongTextJSONReader.java)
   [srcid,partitionid,srcvalue,[[sinkid1,edgeid1,edgevalue1],[sinkid2,edgeid2,edgevalue2]... ]]

   ```
   job.setInputFormat(TextInputFormat.class);
   job.setInputReaderClass(LongTextJSONReader.class);
   ```

   *Note*: Partition ID starts from 0. And if partitionId is specified, the number of input files to the job should be atleast as many as there are partitions.

4. To run the sample, use the following command:

    ```
    hama jar hama-graph-x.x.x-SNAPSHOT-jar-with-dependencies.jar in.dream_lab.goffish.sample.XXXJob input-path output-path 
    ```

    where input-path and output-path is the path of the graph in HDFS and path of the     output in HDFS, 	respectively; e.g:

    ```
    hama jar hama-graph-0.7.2-SNAPSHOT-jar-with-dependencies.jar in.dream_lab.goffish.sample.ConnectedComponentsJob facebook_graph fbout
    ```
