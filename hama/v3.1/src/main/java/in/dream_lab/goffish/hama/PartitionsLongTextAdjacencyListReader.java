/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.hama;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import in.dream_lab.goffish.hama.utils.DisjointSets;
import in.dream_lab.goffish.hama.Message.MessageType;


/* Reads graph in the adjacency list format:
 * VID PartitionID Sink1 Sink2 ...
 */
public class PartitionsLongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable> 
 implements IReader <Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {
  
  public static final Log LOG = LogFactory.getLog(PartitionsLongTextAdjacencyListReader.class);
  
  //TODO : Change to long
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  //TODO : Change to Long from LongWritable
  private Map<LongWritable, LongWritable> vertexSubgraphMap;
  
  public PartitionsLongTextAdjacencyListReader(BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }
  
  
  /*
   * Returns the list of subgraphs belonging to the current partition 
   */
  @Override
  public  List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
    
    Map<Integer, List<String>> partitionMap = new HashMap<Integer, List<String>>();
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    long edgeCount = 0;
    LOG.debug("SETUP Starting " + peer.getPeerIndex() + " Memory: " + Runtime.getRuntime().freeMemory());
    
    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      //NOTE: Confirm that data starts from value and not from key.
      String stringInput = pair.getValue().toString();
      String vertexValue[] = stringInput.split("\\s+");
      //LongWritable sourceID = new LongWritable(Long.parseLong(value[0]));
      int partitionID = Integer.parseInt(vertexValue[1]);

      // Vertex does not belong to this partition
      if (partitionID != peer.getPeerIndex()) {
        List<String> partitionVertices = partitionMap.get(partitionID);
        if (partitionVertices == null) {
          partitionVertices = new ArrayList<String>();
          partitionMap.put(partitionID, partitionVertices);
        }
        partitionVertices.add(stringInput);
      } else {
        LongWritable vertexID = new LongWritable(Long.parseLong(vertexValue[0]));
        List<IEdge<E, LongWritable, LongWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

        for (int j = 2; j < vertexValue.length; j++) {
          LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
          LongWritable edgeID = new LongWritable(
              edgeCount++ | (((long) peer.getPeerIndex()) << 32));
          IEdge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(
              edgeID, sinkID);
          _adjList.add(e);
          _edges.add(e);
        }
        vertexMap.put(vertexID, createVertexInstance(vertexID, _adjList));
      }
    }

    // Send vertices to their respective partitions
    for (Map.Entry<Integer, List<String>> entry : partitionMap.entrySet()) {
      int partitionID = entry.getKey().intValue();
      List<String> vertices = entry.getValue();
      for (String vertex : vertices) {
        Message<LongWritable, LongWritable> vertexMsg = new Message<LongWritable, LongWritable>();
        ControlMessage controlInfo = new ControlMessage();
        controlInfo
            .setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        controlInfo.setVertexValues(vertex);
        vertexMsg.setControlInfo(controlInfo);
        peer.send(peer.getPeerName(partitionID), (Message<K, M>) vertexMsg);
      }
    }
    

    // End of first superstep.
    peer.sync();
    
    LOG.debug("Second Superstep in Reader " + peer.getPeerIndex() + " Memory: " + Runtime.getRuntime().freeMemory());
    
    Message<LongWritable, LongWritable> msg;
    while ((msg = (Message<LongWritable, LongWritable>) peer.getCurrentMessage()) != null) {
      ControlMessage ctrlMessage = (ControlMessage) msg.getControlInfo();
      String msgString = ctrlMessage.getVertexValues();
      // String msgStringArr[] = msgString.split(",");
      // for (int i = 0; i < msgStringArr.length; i++) {
      String vertexInfo[] = msgString.split("\\s+");
      LongWritable vertexID = new LongWritable(Long.parseLong(vertexInfo[0]));
      List<IEdge<E, LongWritable, LongWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

      for (int j = 2; j < vertexInfo.length; j++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(vertexInfo[j]));
        LongWritable edgeID = new LongWritable(
            edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(
            edgeID, sinkID);
        _adjList.add(e);
        _edges.add(e);
      }
      vertexMap.put(vertexID, createVertexInstance(vertexID, _adjList));

    }
    //}
    
    LOG.debug("Creating Remote Vertex Objects");
    
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexId();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }

    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex());

    LOG.debug("Calling formSubgraph()");

    formSubgraphs(partition, vertexMap.values());

    LOG.debug("Done with formSubgraph()");
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent: partitionID remotevertex1 remotevertex2 ...
     */
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionIDbytes);
    for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
      if (v instanceof RemoteVertex) {
        byte vertexIDbytes[] = Longs.toByteArray(v.getVertexId().get());
        controlInfo.addextraInfo(vertexIDbytes);
      }
    }
    sendToAllPartitions(question);

    peer.sync();

    Map<Integer, List<Message<LongWritable, LongWritable>>> replyMessages = new HashMap<Integer, List<Message<LongWritable, LongWritable>>>();
    // Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();

        Integer partitionID = Ints.fromByteArray(subgraphList.iterator().next().getBytes());

        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,1)) {
          LongWritable subgraphID = new LongWritable(
              Longs.fromByteArray(subgraphListElement.getBytes()));
          subgraphPartitionMap.put((K) subgraphID, partitionID);
        }
        continue;
      }

      /*
       * receiving query to find subgraph id Remote Vertex
       */
      Iterable<BytesWritable> RemoteVertexQuery = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();

      /*
       * Reply format : sinkID1 subgraphID1 sinkID2 subgraphID2 ...
       */
      Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>(); 
      controlInfo = new ControlMessage();
      controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
      subgraphIDReply.setControlInfo(controlInfo);

      Integer sinkPartition = Ints.fromByteArray(RemoteVertexQuery.iterator().next().getBytes());
      boolean hasAVertex = false;
      for (BytesWritable remoteVertex : Iterables.skip(RemoteVertexQuery,1)) {
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        // In case this partition does not have the vertex
        /*
         * Case 1 : If vertex does not exist Case 2 : If vertex exists but is
         * remote, then its subgraphID is null
         */
        if (sinkSubgraphID == null) {
          continue;
        }
        hasAVertex = true;
        byte sinkIDbytes[] = Longs.toByteArray(sinkID.get());
        controlInfo.addextraInfo(sinkIDbytes);
        byte subgraphIDbytes[] = Longs.toByteArray(sinkSubgraphID.get());
        controlInfo.addextraInfo(subgraphIDbytes);
      }
      if (hasAVertex) {
        peer.send(peer.getPeerName(sinkPartition.intValue()),
            (Message<K, M>) subgraphIDReply);
      }
    }
    peer.sync();
    
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();

      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      while (queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink =(RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertexMap.get(sinkID);
        assert (sink != null);
        sink.setSubgraphID(remoteSubgraphID);
      }
    }

    return partition.getSubgraphs();
  }

  private IVertex<V, E, LongWritable, LongWritable> createVertexInstance(LongWritable vertexID, List<IEdge<E, LongWritable, LongWritable>> adjList) {
    return ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }

  /*
   * takes partition and message list as argument and sends the messages to
   * their respective partition. Needed to send messages just before
   * peer.sync(),as a hama bug causes the program to stall while trying to send
   * and recieve(iterate over recieved message) large messages at the same time
   */
  private void sendMessage(int partition,
      List<Message<LongWritable, LongWritable>> messageList)
      throws IOException {

    for (Message<LongWritable, LongWritable> message : messageList) {
      peer.send(peer.getPeerName(partition), (Message<K, M>) message);
    }

  }

  private void sendToAllPartitions(Message<LongWritable, LongWritable> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }

  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, Collection<IVertex<V, E, LongWritable, LongWritable>> vertices) throws IOException {

    long subgraphCount = 0;
    Set<LongWritable> visited = new HashSet<LongWritable>();
    Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();
    
    subgraphLocationBroadcast.setMessageType(MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo
        .setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);
    
    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);
    
        // initialize disjoint set
    DisjointSets<IVertex<V, E, LongWritable, LongWritable>> ds = new DisjointSets<IVertex<V, E, LongWritable, LongWritable>>(
        vertices.size());
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertices) {
      ds.addSet(vertex);
    }

    // union edge pairs
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertices) {
      if (!vertex.isRemote()) {
        for (IEdge<E, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
          IVertex<V, E, LongWritable, LongWritable> sink = vertexMap
              .get(edge.getSinkVertexId());
          ds.union(vertex, sink);
        }
      }
    }

    Collection<? extends Collection<IVertex<V, E, LongWritable, LongWritable>>> components = ds
        .retrieveSets();

    for (Collection<IVertex<V, E, LongWritable, LongWritable>> component : components) {
      LongWritable subgraphID = new LongWritable(
          subgraphCount++ | (((long) partition.getPartitionId()) << 32));
      Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          peer.getPeerIndex(), subgraphID);
      

      for (IVertex<V, E, LongWritable, LongWritable> vertex : component) {
        subgraph.addVertex(vertex);
        
        // Dont add remote vertices to the VertexSubgraphMap as remote vertex subgraphID is unknown
        if (!vertex.isRemote()) {
          vertexSubgraphMap.put(vertex.getVertexId(), subgraph.getSubgraphId());
        }        
      }
      
      partition.addSubgraph(subgraph);
      
      byte subgraphIDbytes[] = Longs.toByteArray(subgraphID.get());
      controlInfo.addextraInfo(subgraphIDbytes); 
     
    }
    sendToAllPartitions(subgraphLocationBroadcast);
  }
}
