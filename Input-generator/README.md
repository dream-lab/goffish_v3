# Pipeline to generate goffish input from edge list

Steps to generate common input format supported by both goffish-hama and goffish-giraph
### Step 1 - Convert Edge list to metis input format
```sh
$ python snapToMetisInput.py <edge-list> undirected-edge-list.txt metis-input.txt c
```
_Arguments:_-
1. edge-list  -  The input Snap file 1
2. undirected-edge-list.txt
3. metis-input.txt  -  Contains the partition id of the vertex , line number corresponds to the vertexId in the file
4. c  -  Giraph Input

### Step 2 - Run Metis to partition the input
```sh
$ gpmetis metis-input.txt <number-of-partitions>
```
Metis creates a file which contains the partition id of the vertex with line number corresponding to vertexId

### Step 3 - Add vertexId to Metis output

The part file will be split while passing to mapreduce job therefore we cannot rely on line number
```sh
$ awk '{printf "%d %s\n", NR , $0}' metis-input.part.pno > metis-output
```
Note : 
pno = no of partitions
metis-input.part.pno = metis output file

### Step 4 - Place the files in hdfs
```sh
$ hdfs dfs -put metis-input.txt
$ hdfs dfs -put undirected-edge-list.txt
```

### Step 5 - run the hadoop pipeline
```sh
$ hadoop jar cc-1.0-jar-with-dependencies.jar in.dream_lab.hadoopPipeline.cc.PartitionInputReader <output-dir> 1 undirected-edge-list.txt metis-output <number-of-partitions>
```
Job6 - works with FullInfoNonSplitReader - expects all vertices in same partition to be in a single file (input format = NonSplitTextInputFormat)  
Job5 - works with FullInfoSplitReader - vertices are transfered to their correct partition in first superstep
