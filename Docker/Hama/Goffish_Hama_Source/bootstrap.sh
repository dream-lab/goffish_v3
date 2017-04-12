#!/bin/bash

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

service sshd start

if [[ $1 = "-namenode" || $2 = "-namenode" ]]; then
  service dnsmasq start
  #$HADOOP_PREFIX/sbin/start-dfs.sh
  $HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode
  $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode
  $HADOOP_PREFIX/sbin/start-yarn.sh
  $HAMA_HOME/bin/hama-daemon.sh --config "${HAMA_CONF_DIR}" start zookeeper
  $HAMA_HOME/bin/hama-daemon.sh --config "${HAMA_CONF_DIR}" start bspmaster
fi

if [[ $1 = "-datanode" || $2 = "-datanode" ]]; then
  HN=`hostname`
  IP=`ifconfig eth0 | grep "inet addr" | awk -F: '{print $2}' | awk '{print $1}'`
  ssh namenode "grep -q -F $HN /etc/hosts || ( echo $IP $HN >> /etc/hosts && service dnsmasq restart )"

  NN=`grep namenode /etc/hosts | awk '{print $1}'`
  grep namenode /etc/resolv.conf && echo nameserver $NN > /etc/resolv.conf

  #$HADOOP_PREFIX/sbin/start-dfs.sh
  $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode
  $HADOOP_PREFIX/sbin/yarn-daemons.sh start nodemanager
  $HAMA_HOME/bin/hama-daemon.sh --config "${HAMA_CONF_DIR}" start groom
fi

if [[ $1 = "-d" || $2 = "-d" ]]; then
  while true; do ssh namenode cat /etc/hosts | grep -v localhost | grep -v :: | grep -v namenode | grep -v `hostname` | while read line ; do grep "$line" /etc/hosts > /dev/null 2>&1 || (echo "$line" >> /etc/hosts); done ; sleep 60 ; done
fi

if [[ $1 = "-bash" || $2 = "-bash" ]]; then
  /bin/bash
fi


if [[ $1 = "-namenode" || $2 = "-namenode" ]]; then
  RUN service  sshd start  && $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh && $HADOOP_PREFIX/sbin/start-dfs.sh && $HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/root/ 
 
fi
