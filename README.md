# GoFFish v3

GoFFish is based on our EuroPar 2013 paper. GoFFish v3 is a complete re-write of GoFFish v2 (https://github.com/usc-cloud/goffish/tree/master/v2.0RC2), that was earlier developed at USC. It is a component centric programming model, similar to Google's Pregel (and its Apache Giraph open source implementation), but uses a weakly connected component (subgraph, rather than a vertex) as a unit of computing. This benefits with fewer supersteps for convergence, and reduced messaging between vertices.  This has shown to out-perform Apache Giraph for several graph algorithms. GoFFish is conceptually similar to Giraph++ and Blogel, but using subgraph (not partitions) as a unit of computing, and implemented in Java (not C++) with native support for HDFS and integration with Apache Hama/Giraph.
- Simmhan, Y.; Kumbhare, A.; Wickramaarachchi, C.; Nagarkar, S.; Ravi, S.; Raghavendra, C. & Prasanna, V., GoFFish: A Sub-Graph Centric Framework for Large-Scale Graph Analytics, International European Conference on Parallel Processing (EuroPar), 2014, http://link.springer.com/chapter/10.1007/978-3-319-09873-9_38
- GoFFish Tech Report: https://arxiv.org/pdf/1311.5949.pdf

GoFFish v3, implemented at the DREAM:Lab at IISc, Bangalore, uses a similar subgraph-centric API as earlier versions but is implemented using existing distributed processing engines to improve manageability. Two implementation, using Apache Hama and Apache Giraph, are available, along with several new graph algorithms implemented. There are also easy deployment options using Docker. GoFFish v3 uses HDFS instead of GoFS, and is focussed on single large graph. 

Contact 'goffish-user@googlegroups.com' for comments on the GoFFish v3 project, or Yogesh Simmhan (simmhan@cds.iisc.ac.in) for further details.

GoFFish v3 was primarilly implemented by:
- Diptanshu Kakwani
- Himanshu Sharma
- Anirudh Singh Shekhawat
- Abdul Rabbani Shah

Other contributors to GoFFish v3 development include:
- Ravikant Dindokar
- Jayanth Kalyanasundaram
- Abhilash Sharma
- Sarthak Sharma
- Yogesh Simmhan

Other Publications related to GoFFish include:
- Yogesh Simmhan, Neel Choudhury, Charith Wickramaarachchi, Alok Kumbhare, Marc Frincu, Cauligi Raghavendra and Viktor Prasanna, Distributed Programming over Time-series Graphs, IEEE International Parallel & Distributed Processing Symposium (IPDPS), 2015, http://ieeexplore.ieee.org/document/7161567/
- Nitin Jamadagni and Yogesh Simmhan, GoDB: From Batch Processing to Distributed Querying over Property Graphs, IEEE/ACM International Symposium on Cluster, Cloud, and Grid Computing (CCGrid) , 2016, http://ieeexplore.ieee.org/document/7515700/
- Ravikant Dindokar, Neel Choudhury and Yogesh Simmhan, A Meta-graph Approach to Analyze Subgraph-centric Distributed Programming Models, IEEE International Conference on Big Data (Big Data), 2016, http://ieeexplore.ieee.org/document/7840587/
- Ravikant Dindokar and Yogesh Simmhan, Elastic Partition Placement for Non-stationary Graph Algorithms, IEEE/ACM International Symposium on Cluster, Cloud, and Grid Computing (CCGrid), 2016, http://ieeexplore.ieee.org/document/7515673/
- Badam, N.C. & Simmhan, Y., Subgraph Rank: PageRank for SubgraphCentric Distributed Graph Processing, International Conference on Management of Data (COMAD), 2014, http://dl.acm.org/citation.cfm?id=2726979


Acknowledgements:
- Development of GoFFish is supported by grants from NetApp Inc. and Microsoft Azure for Research
