#!/bin/bash

if [ $# -eq 0 ]; then
   clear
   echo "USAGE:"
   echo ""; echo ""; echo "";
   echo "./goffish_cluster.sh  start                     --To Start a new Goffish HAMA Cluster (Default: 4 Workers)"; echo "";
   echo "./goffish_cluster.sh  stop                     --To Stop the Goffish HAMA Cluster"; echo "";
   echo "./goffish_cluster.sh  scale  <No. of Workers>   --To Scale IN or OUT the Workers"; echo "";
   echo "./goffish_cluster.sh  visualise                 --To Start a Dashboard for Swarm Cluster"; echo "" ;
   echo "./goffish_cluster.sh  login                     --To login to the Master Node"; echo "" ;
elif [ $1 == "start" ]; then
   docker service ls | grep namenode
   if [ $? -eq 0 ]; then
       clear
       echo "HAMA CLUSTER ALREADY RUNNING !! "
   else
       sudo docker stack deploy --compose-file=docker-compose.yml hama_source_cluster
       
   fi
elif [ $1 == "scale" ]; then
   docker service scale hama_cluster_datanode=$2
elif [ $1 == "login" ]; then
   X=`docker ps | grep -oh "\w*namenode.*"`
   docker exec -it $X /bin/bash --login
elif [ $1 == "visualise" ]; then
   docker ps | grep manomarks/visualizer
   if [ $? -ne 0 ]; then
       docker run -it -d -p 4000:8080 -v /var/run/docker.sock:/var/run/docker.sock manomarks/visualizer
   fi
   clear
   echo "Visualiser Running on Port 4000 ..."
   echo "Opening in Browser ..."
   firefox 0.0.0.0:4000  2>1  &
elif [ $1 == "stop" ]; then
   docker stack rm hama_source_cluster

elif [ $1 == "--help" ]; then
   clear
   echo "USAGE:"
   echo ""; echo ""; echo "";
   echo "./goffish_cluster.sh  start                     --To Start a new Goffish HAMA Cluster (Default: 4 Workers)"; echo "";
   echo "./goffish_cluster.sh  stop                     --To Stop the Goffish HAMA Cluster"; echo "";
   echo "./goffish_cluster.sh  scale  <No. of Workers>   --To Scale IN or OUT the Workers"; echo "";
   echo "./goffish_cluster.sh  visualise                 --To Start a Dashboard for Swarm Cluster"; echo ""
   echo "./goffish_cluster.sh  login                     --To login to the Master Node"; echo "" ;
fi




