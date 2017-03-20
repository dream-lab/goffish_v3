Installation
============
Setting up Giraph
-----------------
Fetching Giraph distribution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We would be fetching the Giraph source from GitHub and building it from the source code. Execute the following command to clone the giraph repository into your current directory.

.. code-block:: bash

    git clone https://github.com/apache/giraph.git

Switching to release-1.2 branch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Now that we have cloned the Giraph repository, let's switch to the commit having code for the version of Giraph we are interested in. Run the following commands to do that.

.. code-block:: bash

    cd giraph
    git checkout release-1.2

Modifying pom.xml
~~~~~~~~~~~~~~~~~
We would be building Giraph for Hadoop v2.7.2 with YARN support. Trying to compile the source code right away would result  in a compilation error stating that ``SASL_PROPS`` symbol could not be found. To get around this we would need to remove the ``STATIC_SASL_SYMBOL`` munge symbol under hadoop_yarn profile in ``pom.xml``. Open your favourite text editor and edit the line ``<munge.symbols>PURE_YARN,STATIC_SASL_SYMBOL</munge.symbols>`` to the following

.. code-block:: xml

    <munge.symbols>PURE_YARN</munge.symbols>

Under the `hadoop_yarn` profile, add the version tags to the ``hadoop-common, hadoop-mapreduce-client-common, hadoop-mapreduce-client-core`` dependencies.

.. code-block:: xml

    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-common</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-common</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-core</artifactId>
        <version>${hadoop.version}</version>
    </dependency>

Setting up GoFFish-Giraph
-------------------------
Fetch the GoFFish-Giraph's source from the git repository

.. code-block:: bash

    git clone https://github.com/dream-lab/goffish_v3.git

Move ``goffish-api`` and ``goffish-giraph`` directories into the ``giraph`` directory.

Apply patch file
~~~~~~~~~~~~~~~~
Apply the patch to the GiraphConfigurationValidator class.

.. code-block:: bash

    patch giraph/giraph-core/src/main/java/org/apache/giraph/job/GiraphConfigurationValidator.java GiraphConfigurationValidator.patch

Adding GoFFish modules
~~~~~~~~~~~~~~~~~~~~~~
Add the new modules to the root ``pom.xml``.

.. code-block:: xml

    <modules>
      <module>giraph-core</module>
      <module>giraph-block-app</module>
      <module>giraph-examples</module>
      <module>goffish-giraph</module>
      <module>goffish-api</module>
    </modules>
    
Building GoFFish-Giraph from source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We use Maven to build GoFFish-Giraph with the following command, specifying the profile ``hadoop_yarn`` and hadoop version ``2.7.2`` as command line parameters.

.. code-block:: bash

    mvn –Phadoop_yarn –Dhadoop.version=2.7.2 -DskipTests clean package
    
Sample Command
--------------
  
.. code-block:: bash
  
    hadoop jar goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dgiraph.metrics.enable=true in.dream_lab.goffish.giraph.graph.GiraphSubgraphComputation -vif in.dream_lab.goffish.giraph.formats.LongDoubleDoubleAdjacencyListSubgraphInputFormat -vip anirudh-stuff/subgraph-input-small -vof in.dream_lab.goffish.giraph.formats.SubgraphSingleSourceShortestPathOutputFormatSir -op anirudh-stuff/output/blehfinal -ca giraph.subgraphVertexValueClass=org.apache.hadoop.io.DoubleWritable,subgraphSourceVertex=1,giraph.subgraphMessageValueClass=org.apache.hadoop.io.BytesWritable,giraph.outgoingMessageValueFactoryClass=in.dream_lab.goffish.giraph.factories.DefaultSubgraphMessageFactory,giraph.messageEncodeAndStoreType=POINTER_LIST_PER_VERTEX,giraph.clientSendBufferSize=20000000,giraph.clientReceiveBufferSize=20000000,giraph.graphPartitionerFactoryClass=in.dream_lab.goffish.giraph.factories.SubgraphPartitionerFactory,giraph.subgraphValueClass=org.apache.hadoop.io.LongWritable,giraph.vertexClass=in.dream_lab.goffish.giraph.graph.DefaultSubgraph,subgraphComputationClass=in.dream_lab.goffish.giraph.examples.SingleSourceShortestPath,giraph.edgeValueClass=org.apache.hadoop.io.DoubleWritable,giraph.vertexIdClass=in.dream_lab.goffish.giraph.graph.SubgraphId,giraph.vertexValueClass=in.dream_lab.goffish.giraph.graph.SubgraphVertices,giraph.outgoingMessageValueClass=in.dream_lab.goffish.giraph.graph.SubgraphMessage -yj goffish-giraph-1.2.0-RC0-for-hadoop-2.6.0-jar-with-dependencies.jar -mc in.dream_lab.goffish.giraph.master.SubgraphMasterCompute -w 4 -yh 4000
    

