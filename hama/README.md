#GoFFish on Hama

Follow the steps to install GoFFish

1. **Install Hadoop and Hama using this [link](http://people.apache.org/~tjungblut/downloads/hamadocs/ApacheHamaInstallationGuide_06.pdf).**

2. **Build**

    ```
    git clone https://github.com/dream-lab/goffish_v3
    cd goffish/api
    mvn clean install						# Build api project
    cd ../sample
    mvn clean install						# Build sample project
    cd ../hama/v3.1/
    mvn clean install assembly:single		# Build goffish-hama
    ```

3. **Graphs**

   3.1 [Generate random graph using hama-examples](https://hama.apache.org/run_examples.html):

   ```
   $HAMA_HOME/bin/hama jar hama-examples-x.x.x.jar gen fastgen -v 100 -e 10 -o randomgraph -t 2
   ```

   3.2 Supported graph formats

   3.2.1 *Adjacency List (LongTextAdjacencyListReader.java)*

   ​
   *srcId sinkVertexId sinkVertexId*

   ```
   job.setInputFormat(NonSplitTextInputFormat.class);
   job.setInputReaderClass(LongTextAdjacencyListReader.class);
   ```

   If no reader is specified in the job, this is used as default reader.

   3.2.2 *Partitioned Adjacency List ( PartitionsLongTextAdjacencyReader.java)*

   ​
   *srcId partitionID sinkVertexId sinkVertexId*

   ```
   job.setInputFormat(TextInputFormat.class);
   job.setInputReaderClass(PartitionsLongTextAdjacencyReader.class);
   ```
    3.2.3 *JSON Reader (LongTextJSONReader.java)*

   ​
   *[srcid,partitionid,srcvalue,[[sinkid1,edgeid1,edgevalue1],[sinkid2,edgeid2,edgevalue2]... ]]*

   ```
   job.setInputFormat(TextInputFormat.class);
   job.setInputReaderClass(LongTextJSONReader.class);
   ```

   *Note*: Partition ID starts from 0. And if partitionId is specified, the number of input files to the job should be atleast as many as there are partitions.

4. **Running sample**

   Place the jars generated in step 2 in Hama classpath:

   ```
   cd goffish_v3/
   cp -t $HAMA_HOME/lib/ api/target/goffish-api-3.1.jar sample/target/goffish-sample-3.1.jar hama/v3.1/target/goffish-hama-3.1-jar-with-dependencies.jar
   ```

   General format of running goffish-hama job:

    ```
   hama jar goffish-hama-3.1-jar-with-dependencies.jar in.dream_lab.goffish.sample.XXXJob properties-file input-path output-path 
    ```

    where input-path and output-path is the path of the graph in HDFS and path of the  output in HDFS, 	respectively and properties-file is local file used for loading properties of job; e.g:

    ```
   hama jar goffish-hama-3.1-jar-with-dependencies.jar in.dream_lab.goffish.sample.DefaultJob ConnectedComponents.properties facebook_graph fbout
    ```
   Output and logs can be found at $HAMA_HOME/logs/tasklogs/job_id/

5. **Writing custom application**

   Your application has to extend AbstractSubgraphComputation.java and implement the compute function. The job configuration (equivalent of driver class in MapReduce)  can be written in the form of properties file or as a Java class. See DefaultJob.java and ConnectedComponents.properties for more details.

   Add the following dependencies to your project in pom.xml:

   ```
   <dependency>
   	<groupId>in.dream_lab.goffish</groupId>
   	<artifactId>goffish-api</artifactId>
   	<version>3.1</version>
   </dependency>
   <dependency>
   	<groupId>in.dream_lab.goffish</groupId>
   	<artifactId>goffish-hama</artifactId>
   	<version>3.1</version>
   </dependency>
   ```

   Your application can be run in the similar way as described above for the sample job.