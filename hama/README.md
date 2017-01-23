#GoFFish on Hama

Follow the steps to install GoFFish

1. Install Hadoop and Hama using the this [link](http://people.apache.org/~tjungblut/downloads/hamadocs/ApacheHamaInstallationGuide_06.pdf).
2.  Clone and build using the following commands:

```
git clone https://github.com/dream-lab/goffish_v3
cd goffish_v3/hama/v0.1/
mvn clean install
```

3. To run the sample, use the following command:

```
hama jar hama-graph-x.x.x-SNAPSHOT.jar in.dream_lab.goffish.sample.XXXJob input-path output-path 
```

where input-path and output-path is the path of the graph in HDFS and path of the output in HDFS, 	respectively; e.g:

```
hama jar hama-graph-0.7.2-SNAPSHOT.jar in.dream_lab.goffish.sample.ConnectedComponentsJob facebook_graph fbout
```

