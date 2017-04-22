# GoFFish Hama Bin Docker

The bin image contains the pre-built jar for GoFFish-Hama directly available for execution. It is dependent on the base image, so make sure you've built or pulled the base image before you move on to bin. 

Follow these steps to build and run the image

## Build it from source
 
    cd goffish_v3/Docker/Hama/Goffish_Hama_Bin
    
    docker build -t dreamlab/goffish3-hama-bin .
   
### or

## Pull the image from Dockerhub
    
    You can always fetch the latest image for use, directly from the DockerHub.
    
    https://hub.docker.com/r/dreamlab/goffish3-hama-bin/




# Running the GoFFish Hama Cluster

You have several options to run this image:
- Running nodes one by one on local machine

- Running cluster on Docker Swarm Cluster

## 1. Running nodes one by one
### 1.1. Start the Master container

In order to use the Docker image you have just build or pulled use:

```
sudo docker run -it --name namenode -h namenode dreamlab/goffish3-hama-bin  /etc/bootstrap.sh -bash -namenode
```

### 1.2. Start the Slave/Worker container

In order to add worker nodes to the GoFFish cluster, use:

```
sudo docker run -it --link namenode:namenode --dns=namenode dreamlab/goffish3-hama-bin /etc/bootstrap.sh -bash -datanode
```

## 2. Deploy the cluster to Docker Swarm.
In order to run GoFFish Cluster in distributed mode with Docker Compose (Version 3), first you need to prepare a [swarm cluster](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/).


Now we are ready to create an application private overlay network. Overlay network is the one that Docker will handle across distributed machines. It requires creating Consul or Zookeper to preserve state.

```
docker network create --driver overlay --subnet=10.0.9.0/24 appnet
```


Once the Swarm cluster is set up, use the [goffish_cluster.sh](https://github.com/dream-lab/goffish_v3/blob/master/Docker/Hama/Goffish_Hama_Bin/goffish_cluster.sh) on the Master Node to start, login, scale, visualise and stop the cluster.

```
./goffish_cluster.sh start                              // To start the Cluster

./goffish_cluster.sh login                              // To login to the Master Node

./goffish_cluster.sh scale (No. of Workers)             // To scale out and add new workers

./goffish_cluster.sh visualise                          // To Start a Dashboard for Swarm Cluster

./goffish_cluster.sh stop                               // To stop the Cluster

```


# Executing a Sample GoFFish Example
## 1. Log into the Master Node
```

./goffish_cluster.sh login

```
## 2. Load the Graphs to HDFS and view execution instructions

It will show a list of algorithms available from the jar and list of available graphs from hdfs along with the instructions for execution.

```
goffish init

```
## 3. Run the sample GoFFish example from the List available to you
```
goffish run VertexCount /facebook-4P

```
