/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  @author Himanshu Sharma
 *  @author Diptanshu Kakwani
*/

package in.dream_lab.goffish.hama;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import in.dream_lab.goffish.hama.utils.DisjointSets;
import in.dream_lab.goffish.hama.Message.MessageType;

/*
 * Reads the graph from JSON format
 * [srcid,pid, srcvalue, [[sinkid1,edgeid1,edgevalue1], [sinkid2,edgeid2,edgevalue2] ... ]] 
 */

public class LongTextJSONReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  HamaConfiguration conf;
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

  public LongTextJSONReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    // Map of partitionID,vertex that do not belong to this partition
    Map<Integer, List<String>> partitionMap = new HashMap<Integer, List<String>>();

    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    // List of edges.Used to create RemoteVertices
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String StringJSONInput = pair.getValue().toString();
      JSONArray JSONInput = (JSONArray) JSONValue.parse(StringJSONInput);

      int partitionID = Integer.parseInt(JSONInput.get(1).toString());

      // Vertex does not belong to this partition
      if (partitionID != peer.getPeerIndex()) {
        List<String> partitionVertices = partitionMap.get(partitionID);
        if (partitionVertices == null) {
          partitionVertices = new ArrayList<String>();
          partitionMap.put(partitionID, partitionVertices);
        }
        partitionVertices.add(StringJSONInput);
      } else {
        IVertex<V, E, LongWritable, LongWritable> vertex = createVertex(
            StringJSONInput);
        vertexMap.put(vertex.getVertexId(), vertex);
        for (IEdge<E, LongWritable, LongWritable> e : vertex.getOutEdges())
          _edges.add(e);
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
    
    //End of first SuperStep
    peer.sync();
    
    Message<LongWritable, LongWritable> msg;
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      String JSONVertex = msg.getControlInfo().toString();
      IVertex<V, E, LongWritable, LongWritable> vertex = createVertex(JSONVertex);
      vertexMap.put(vertex.getVertexId(), vertex);
      for (IEdge<E, LongWritable, LongWritable> e : vertex.getOutEdges())
        _edges.add(e);
    }
    
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexId();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }
    
    //Direct Copy paste from here
    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex());
    
    formSubgraphs(partition, vertexMap.values());
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent:
     * partitionID remotevertex1 remotevertex2 ...
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
    //Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer.getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast
       * Format of received message:
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
       * Reply format :
       * sinkID1 subgraphID1 sinkID2 subgraphID2 ...
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
        //In case this partition does not have the vertex 
        /* Case 1 : If vertex does not exist
         * Case 2 : If vertex exists but is remote, then its subgraphID is null
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
      while(queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink =(RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertexMap.get(sinkID);
        sink.setSubgraphID(remoteSubgraphID);
      }
    }
    
    return partition.getSubgraphs();
  }
  
  /* takes partition and message list as argument and sends the messages to their respective partition.
   * Needed to send messages just before peer.sync(),as a hama bug causes the program to stall while trying
   * to send and recieve(iterate over recieved message) large messages at the same time
   */
  private void sendMessage(int partition,
      List<Message<LongWritable, LongWritable>> messageList) throws IOException {
    
    for (Message<LongWritable, LongWritable> message : messageList) {
      peer.send(peer.getPeerName(partition), (Message<K, M>)message);
    }
    
  }
  
  private void sendToAllPartitions(Message<LongWritable, LongWritable> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }
  
  @SuppressWarnings("unchecked")
  IVertex<V, E, LongWritable, LongWritable> createVertex(String JSONString) {
    JSONArray JSONInput = (JSONArray) JSONValue.parse(JSONString);

    LongWritable sourceID = new LongWritable(
        Long.valueOf(JSONInput.get(0).toString()));
    assert (vertexMap.get(sourceID) == null);

    List<IEdge<E, LongWritable, LongWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    //fix this
    V value = (V) new Text(JSONInput.get(2).toString());


    JSONArray edgeList = (JSONArray) JSONInput.get(3);
    for (Object edgeInfo : edgeList) {
      Object edgeValues[] = ((JSONArray) edgeInfo).toArray();
      LongWritable sinkID = new LongWritable(
          Long.valueOf(edgeValues[0].toString()));
      LongWritable edgeID = new LongWritable(
          Long.valueOf(edgeValues[1].toString()));
      //fix this
      E edgeValue = (E) new Text(edgeValues[2].toString());
      
      Edge<E, LongWritable, LongWritable> edge = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      edge.setValue(edgeValue);
      _adjList.add(edge);
    }

    IVertex<V, E, LongWritable, LongWritable> vertex = createVertexInstance(sourceID, _adjList);
    vertex.setValue(value);
    return vertex;
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

  private IVertex<V, E, LongWritable, LongWritable> createVertexInstance(LongWritable vertexID, List<IEdge<E, LongWritable, LongWritable>> adjList) {
    return ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }
}
